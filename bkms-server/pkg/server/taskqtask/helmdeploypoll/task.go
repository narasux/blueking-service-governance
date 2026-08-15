/*
 * TencentBlueKing is pleased to support the open source community by making
 * 蓝鲸智云 - 服务治理 (BlueKing Service Governance) available.
 * Copyright (C) Tencent. All rights reserved.
 * Licensed under the MIT License (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *  http://opensource.org/licenses/MIT
 *
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * We undertake not to change the open source license (MIT license) applicable
 * to the current version of the project delivered to anyone in the future.
 */

package helmdeploypoll

import (
	"context"
	"fmt"
	"time"

	"github.com/hibiken/asynq"
	"github.com/pkg/errors"
	"github.com/samber/lo"
	helmrelease "helm.sh/helm/v3/pkg/release"

	"github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/common/config"
	log "github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/common/logging"
	"github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/deploy"
	helmdeploy "github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/deploy/helm"
	"github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/infras/database"
	"github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/infras/helm"
	"github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/infras/taskq"
	"github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/misc/audit"
	"github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/observability/metrics"
	storereg "github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/server/registry"
	"github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/workload/topology"
)

const (
	// name asynq 任务类型名
	name = "taskq.pollingHelmDeployStatus"
	// tickMaxRetry 单次 tick 意外失败的 asynq 重试上限，不含轮询续跑
	tickMaxRetry = 10
	// totalFailureRetryCount 查 Release 状态连续失败次数上限，耗尽后标 StatusPollingBroken
	totalFailureRetryCount = 10
	// saveStatusTimeout 状态落库的独立超时，避免 handler ctx 取消导致写不进去
	saveStatusTimeout = 10 * time.Second
)

// Args Helm 部署 / 回滚状态轮询的业务参数，不含用户身份
type Args struct {
	WorkspaceID        string `json:"workspaceID"`
	AppID              string `json:"appID"`
	EnvName            string `json:"envName"`
	TrafficLaneName    string `json:"trafficLaneName"`
	DeployID           string `json:"deployID"`
	FailureRetryRemain int    `json:"failureRetryRemain,omitempty"`
}

// String 输出轮询身份与剩余失败次数，便于日志对齐同一部署的连续 tick
func (args Args) String() string {
	trafficLaneName := lo.Ternary(args.TrafficLaneName == "", "default", args.TrafficLaneName)
	return fmt.Sprintf(
		"<workspace: %s, appID: %s, envName: %s, trafficLaneName: %s, id: %s, remain: %d>",
		args.WorkspaceID, args.AppID, args.EnvName, trafficLaneName, args.DeployID, args.FailureRetryRemain,
	)
}

// Task 部署状态轮询任务；init 赋值避免与 enqueueNext 引用形成包初始化环
var Task *taskq.TaskType[Args]

func init() {
	Task = taskq.NewTaskType[Args](name, handle, asynq.MaxRetry(tickMaxRetry))
}

// PollingInterval 读 TaskPoller.DeployStatus.Interval（秒），作为下一 tick 的 ProcessIn 延迟
func PollingInterval() time.Duration {
	return time.Duration(config.G.TaskPoller.DeployStatus.Interval) * time.Second
}

// pollingTimeout 读 TaskPoller.DeployStatus.Timeout（秒），从 record.StartedAt 起算轮询窗口
func pollingTimeout() time.Duration {
	return time.Duration(config.G.TaskPoller.DeployStatus.Timeout) * time.Second
}

func handle(ctx context.Context, args Args) error {
	reg := storereg.G()
	if reg == nil || reg.HelmDeployRecordStore == nil {
		log.Errorf(ctx, "helm deploy poll stores not initialized, stop task: %s", args)
		return errors.Wrap(taskq.ErrStopRetry, "helm deploy poll stores not initialized")
	}
	return NewManager(reg.HelmDeployRecordStore).Handle(ctx, args)
}

// Manager 执行一次 Helm 部署状态轮询 tick
type Manager struct {
	recordStore helmdeploy.RecordStore
}

// NewManager 注入部署轮询所需 store，供 asynq handler 与单测共用
func NewManager(recordStore helmdeploy.RecordStore) *Manager {
	return &Manager{recordStore: recordStore}
}

// Handle 执行一次部署状态轮询 tick：读本地记录，必要时查 Release 并落库。
// 已稳定则直接返回；仍在跑则 ProcessIn 投递下一 tick（新任务，retry 从 0 计）。
// asynq MaxRetry(tickMaxRetry) 只约束本 tick 的意外失败（如 enqueue 失败），不约束轮询次数；
// 轮询窗口由 pollingTimeout 截断，查状态失败次数由 FailureRetryRemain 截断。
// 停止轮询且 latest 仍是本记录时释放部署锁；不可恢复错误 wrap taskq.ErrStopRetry。
func (m *Manager) Handle(ctx context.Context, args Args) error {
	if m.recordStore == nil {
		return errors.Wrap(taskq.ErrStopRetry, "helm deploy poll stores not initialized")
	}

	record, err := m.recordStore.Get(ctx, args.AppID, args.DeployID)
	if err != nil {
		return errors.Wrapf(taskq.ErrStopRetry, "get deploy record: %v", err)
	}
	// 迟到或重复 tick：已稳定则不再查状态；若 latest 仍是本记录则补放锁
	if helm.IsStable(record.Status) {
		log.Infof(ctx, "deploy %s already %s, skip tick", args, record.Status)
		m.releaseLockIfLatest(ctx, args)
		return nil
	}
	if time.Since(record.StartedAt) >= pollingTimeout() {
		log.Warnf(ctx, "deploy %s polling window exceeded, mark pollingTimeout", args)
		return m.terminate(ctx, record, args, helm.StatusPollingTimeout, "")
	}

	go triggerTopologyRefresh(context.WithoutCancel(ctx), args, record)

	remain := args.FailureRetryRemain
	if remain <= 0 {
		remain = totalFailureRetryCount
	}

	curStatus := record.Status
	release, err := fetchReleaseStatus(ctx, record)
	if err != nil {
		remain--
		if remain <= 0 {
			log.Errorf(ctx, "stop polling release %s after %d retries", args, totalFailureRetryCount)
			return m.terminate(ctx, record, args, helm.StatusPollingBroken, err.Error())
		}
		log.Errorf(ctx, "fetch deploy %s status failed, remain=%d: %v", args, remain, err)
		return m.enqueueNext(ctx, args, remain)
	}

	deployStatus := release.DeployResult.Status
	if deployStatus != record.Status {
		record.Status = deployStatus
		record.Revision = release.Version
		record.Message = release.DeployResult.Description
		if helm.IsStable(deployStatus) {
			record.EndedAt = time.Now()
		}
	}

	if record.Status != curStatus {
		if err = m.save(ctx, record, args); err != nil {
			log.Errorf(ctx, "failed to update helm deploy record: %v", err)
		}
	}
	if record.Status == helm.StatusUninstalled {
		m.releaseLockIfLatest(ctx, args)
		return nil
	}
	if helm.IsStable(record.Status) {
		return m.onStable(ctx, record, args)
	}
	return m.enqueueNext(ctx, args, remain)
}

// enqueueNext 按配置间隔 ProcessIn 投递下一 tick；新任务 retry 从 0 计
func (m *Manager) enqueueNext(ctx context.Context, args Args, remain int) error {
	args.FailureRetryRemain = remain
	interval := PollingInterval()
	log.Infof(ctx, "schedule next poll for deploy %s in %s remain=%d", args, interval, remain)
	if err := taskq.Enqueue(ctx, Task.NewTask(args), asynq.ProcessIn(interval)); err != nil {
		return errors.Wrap(err, "enqueue next helm deploy poll tick")
	}
	return nil
}

// terminate 把记录标为指定终态并落库；卸载态只放锁，其余走 onStable
func (m *Manager) terminate(
	ctx context.Context,
	record *helmdeploy.Record,
	args Args,
	status helmrelease.Status,
	message string,
) error {
	record.Status = status
	if message != "" {
		record.Message = message
	}
	if record.EndedAt.IsZero() {
		record.EndedAt = time.Now()
	}
	if err := m.save(ctx, record, args); err != nil {
		log.Errorf(ctx, "failed to update helm deploy record: %v", err)
	}
	if record.Status == helm.StatusUninstalled {
		m.releaseLockIfLatest(ctx, args)
		return nil
	}
	return m.onStable(ctx, record, args)
}

// save 落部署记录；库中已是已卸载则不再覆盖
func (m *Manager) save(ctx context.Context, record *helmdeploy.Record, args Args) error {
	saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), saveStatusTimeout)
	defer cancel()

	dbRecord, err := m.recordStore.Get(saveCtx, args.AppID, args.DeployID)
	if err != nil {
		log.Errorf(saveCtx, "failed to get helm deploy record: %v", err)
	} else if dbRecord.Status == helm.StatusUninstalled {
		log.Warnf(
			saveCtx, "helm deploy %s status is %s (from db), stop deploy status polling",
			args, dbRecord.Status,
		)
		record.Status = dbRecord.Status
		return nil
	}

	log.Infof(
		saveCtx, "helm deploy %s status changed to %s, message: %s",
		args, record.Status, record.Message,
	)
	return m.recordStore.Update(saveCtx, record)
}

// onStable 终态副作用失败只打日志，不改部署结果，也不让本 tick 失败
func (m *Manager) onStable(ctx context.Context, record *helmdeploy.Record, args Args) error {
	metrics.DeployFinished(metrics.DeployKindHelm, string(record.Status), record.StartedAt, time.Now())
	log.Infof(ctx, "helm deploy %s status is %s (Stable), stop polling and release lock", args, record.Status)

	if record.Status == helm.StatusDeployed {
		handleDeploySucceeded(ctx, args, record)
	}

	opResult := lo.Ternary(
		record.Status == helm.StatusDeployed,
		audit.ResultSuccess,
		audit.ResultFailed,
	)
	go audit.AddOperationRecordAsync(
		context.WithoutCancel(ctx),
		audit.OperationTypeDeploy, audit.ResourceTypeApp, args.AppID,
		audit.WithResult(opResult), audit.WithWorkspaceID(args.WorkspaceID),
		audit.WithAppID(args.AppID), audit.WithEnvName(args.EnvName),
	)
	m.releaseLockIfLatest(ctx, args)
	return nil
}

// releaseLockIfLatest 仅当 latest 仍是本 deployID 时释放锁，避免 Del 误放更新部署的锁
func (m *Manager) releaseLockIfLatest(ctx context.Context, args Args) {
	latest, err := m.recordStore.GetLatest(ctx, args.AppID, args.EnvName, args.TrafficLaneName)
	if err != nil {
		log.Errorf(ctx, "skip release helm deploy lock for %s: get latest: %v", args, err)
		return
	}
	if latest == nil || latest.ID.Hex() != args.DeployID {
		log.Infof(ctx, "skip release helm deploy lock for %s: latest is no longer this record", args)
		return
	}
	releaseDeployLock(ctx, args)
}

// releaseDeployLock 释放同应用+环境+泳道的部署锁；抽成函数便于单测 mock
func releaseDeployLock(ctx context.Context, args Args) {
	helmdeploy.NewDeployLock(args.AppID, args.EnvName, args.TrafficLaneName).Release(ctx)
}

// fetchReleaseStatus 初始化 Helm action 并查询一次 Release 状态；抽成函数便于单测 mock
func fetchReleaseStatus(ctx context.Context, record *helmdeploy.Record) (*helm.Release, error) {
	debugLog := helm.NewHelmDebugLogger(ctx, record.ReleaseName, "polling-status")
	cfg, err := helm.NewActionConfiguration(record.ClusterID, record.Namespace, debugLog)
	if err != nil {
		return nil, errors.Wrapf(err, "init action configuration for polling %s", record.ReleaseName)
	}
	return helm.GetReleaseStatus(cfg, record.ReleaseName)
}

// triggerTopologyRefresh 按 ReleaseName 刷拓扑资源范围
// 由 Handle 在 goroutine 中调用，releaseName 为空则跳过；失败只打日志，不影响本 tick
func triggerTopologyRefresh(ctx context.Context, args Args, record *helmdeploy.Record) {
	if record.ReleaseName == "" {
		log.Warnf(ctx, "skip topology refresh (helm): releaseName is empty")
		return
	}

	store, err := topology.NewResourceSnapshotStoreMongo(database.Client(), database.Name())
	if err != nil {
		log.Errorf(ctx, "topology refresh (helm): create store: %v", err)
		return
	}
	topology.NewRefresher(store).TriggerRefresh(ctx, topology.RefreshArgs{
		AppID:           args.AppID,
		EnvName:         args.EnvName,
		TrafficLaneName: args.TrafficLaneName,
		ClusterID:       record.ClusterID,
		Namespace:       record.Namespace,
		ReleaseName:     record.ReleaseName,
	})
}

// handleDeploySucceeded 部署成功后的后置动作：记录应用与环境关联，并异步同步该环境告警策略
// 仅 StatusDeployed 时由 onStable 调用；任一步失败只打日志，不改部署结果、不让本 tick 失败
func handleDeploySucceeded(ctx context.Context, args Args, record *helmdeploy.Record) {
	reg := storereg.G()
	if reg == nil {
		log.Errorf(ctx, "track env add app: registry is not initialized")
		return
	}

	log.Infof(
		ctx, "deploy succeeded, start post-deploy hooks for workspace=%s app=%s env=%s lane=%s operator=%s",
		args.WorkspaceID, args.AppID, args.EnvName, args.TrafficLaneName, record.Operator,
	)
	if reg.EnvStore == nil {
		log.Errorf(ctx, "track env add app: env store is not initialized")
	} else {
		deploy.TrackEnvAddApp(ctx, reg.EnvStore, args.WorkspaceID, args.EnvName, args.AppID)
	}

	deploy.SyncAlertStrategiesAfterDeploy(
		ctx, args.WorkspaceID, args.AppID, args.EnvName, args.TrafficLaneName, record.Operator,
	)
}
