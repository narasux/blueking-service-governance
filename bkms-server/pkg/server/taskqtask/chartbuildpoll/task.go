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

package chartbuildpoll

import (
	"context"
	"fmt"
	"time"

	"github.com/hibiken/asynq"
	"github.com/pkg/errors"
	"github.com/samber/lo"

	"github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/bkintegrations/bkci"
	"github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/bkintegrations/bkci/pipelinevar"
	helmchartbuild "github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/build/chart"
	log "github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/common/logging"
	"github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/infras/account/auth"
	bkciapi "github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/infras/cloudapi/bkci"
	"github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/infras/taskq"
	"github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/misc/audit"
	storereg "github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/server/registry"
)

const (
	// name asynq 任务类型名
	name = "taskq.pollingHelmChartBuildStatus"
	// tickMaxRetry 单次 tick 意外失败的 asynq 重试上限，不含轮询续跑
	tickMaxRetry = 10
	// totalFailureRetryCount 查蓝盾连续失败次数上限，耗尽后标 StatusPollingBroken
	totalFailureRetryCount = 10
	// saveStatusTimeout 状态落库的独立超时，避免 handler ctx 取消导致写不进去
	saveStatusTimeout = 10 * time.Second
	// pollingInterval 固定轮询间隔，与现网 ticker 一致
	pollingInterval = 5 * time.Second
	// pollingTimeout 从 record.StartedAt 起算的轮询窗口，超时标 StatusPollingTimeout
	pollingTimeout = 10 * time.Minute
)

// PollingInterval 返回下一 tick 的固定延迟
func PollingInterval() time.Duration {
	return pollingInterval
}

// Args Helm Chart 构建状态轮询的业务参数，不含用户身份
type Args struct {
	WorkspaceID        string `json:"workspaceID"`
	AppID              string `json:"appID"`
	BuildID            string `json:"buildID"`
	FailureRetryRemain int    `json:"failureRetryRemain,omitempty"`
}

// String 输出轮询身份与剩余失败次数，便于日志对齐同一构建的连续 tick
func (args Args) String() string {
	return fmt.Sprintf(
		"<workspace: %s, appID: %s, buildID: %s, remain: %d>",
		args.WorkspaceID, args.AppID, args.BuildID, args.FailureRetryRemain,
	)
}

// Task Chart 构建状态轮询任务；init 赋值避免与 enqueueNext 引用形成包初始化环
var Task *taskq.TaskType[Args]

func init() {
	Task = taskq.NewTaskType[Args](name, handle, asynq.MaxRetry(tickMaxRetry))
}

// handle asynq 入口：registry / 必要 store 缺失则打日志并 ErrStopRetry，否则交给 Manager
func handle(ctx context.Context, args Args) error {
	reg := storereg.G()
	if reg == nil ||
		reg.HelmChartBuildRecordStore == nil ||
		reg.BkCIPipelineStore == nil {
		log.Errorf(ctx, "chart build poll stores not initialized, stop task: %s", args)
		return errors.Wrap(taskq.ErrStopRetry, "chart build poll stores not initialized")
	}
	return NewManager(reg.HelmChartBuildRecordStore, reg.BkCIPipelineStore).Handle(ctx, args)
}

// Manager 执行一次 Chart 构建状态轮询 tick
type Manager struct {
	recordStore   helmchartbuild.RecordStore
	pipelineStore bkci.PipelineStore
}

// NewManager 注入 Chart 构建轮询所需 store，供 asynq handler 与单测共用
func NewManager(recordStore helmchartbuild.RecordStore, pipelineStore bkci.PipelineStore) *Manager {
	return &Manager{recordStore: recordStore, pipelineStore: pipelineStore}
}

// Handle 执行一次 Chart 构建状态轮询 tick：读本地记录，必要时查蓝盾并落库。
// 记录已终态则直接返回；仍在跑则 ProcessIn 投递下一 tick（新任务，retry 从 0 计）。
// asynq MaxRetry(tickMaxRetry) 只约束本 tick 的意外失败（如 enqueue 失败），不约束轮询次数；
// 轮询窗口由 pollingTimeout 截断，查蓝盾失败次数由 FailureRetryRemain 截断。
// 不可恢复错误 wrap taskq.ErrStopRetry，避免 asynq 空转重试。
func (m *Manager) Handle(ctx context.Context, args Args) error {
	if m.recordStore == nil || m.pipelineStore == nil {
		return errors.Wrap(taskq.ErrStopRetry, "chart build poll stores not initialized")
	}

	record, err := m.recordStore.Get(ctx, args.AppID, args.BuildID)
	if err != nil {
		return errors.Wrapf(taskq.ErrStopRetry, "get chart build record: %v", err)
	}
	if record.IsTerminated() {
		log.Infof(ctx, "chart build %s already terminated as %s, skip tick", args, record.Status)
		return nil
	}
	if time.Since(record.StartedAt) >= pollingTimeout {
		log.Warnf(ctx, "chart build %s polling window exceeded, mark pollingTimeout", args)
		return m.terminate(ctx, record, args, helmchartbuild.StatusPollingTimeout)
	}

	user, err := auth.GetUser(ctx)
	if err != nil {
		return errors.Wrapf(taskq.ErrStopRetry, "get authed user: %v", err)
	}
	apiClient, err := bkciapi.New(user)
	if err != nil {
		return errors.Wrapf(taskq.ErrStopRetry, "create bkci api client: %v", err)
	}
	pipeline, err := m.pipelineStore.GetByWorkspaceAndType(
		ctx, args.WorkspaceID, string(bkci.PipelineTypeHelmGitBuild),
	)
	if err != nil {
		return errors.Wrapf(
			taskq.ErrStopRetry, "get workspace %s helm-git-build pipeline: %v", args.WorkspaceID, err,
		)
	}

	remain := args.FailureRetryRemain
	if remain <= 0 {
		remain = totalFailureRetryCount
	}

	curStatus := record.Status
	if err = fetchAndUpdateChartBuildRecord(ctx, apiClient, pipeline, record, args.BuildID); err != nil {
		remain--
		if remain <= 0 {
			log.Errorf(ctx, "stop polling chart build %s after %d retries", args, totalFailureRetryCount)
			return m.terminate(ctx, record, args, helmchartbuild.StatusPollingBroken)
		}
		log.Errorf(ctx, "fetch chart build %s status failed, remain=%d: %v", args, remain, err)
		return m.enqueueNext(ctx, args, remain)
	}

	if record.Status != curStatus {
		log.Infof(ctx, "chart build %s status changed from %s to %s", args, curStatus, record.Status)
		if err = m.save(ctx, record); err != nil {
			log.Errorf(ctx, "failed to update chart build record: %v", err)
		}
	}
	if record.IsTerminated() {
		return m.onTerminated(ctx, record, args)
	}
	return m.enqueueNext(ctx, args, remain)
}

// enqueueNext 按固定间隔 ProcessIn 投递下一 tick；新任务 retry 从 0 计
func (m *Manager) enqueueNext(ctx context.Context, args Args, remain int) error {
	args.FailureRetryRemain = remain
	interval := PollingInterval()
	log.Infof(ctx, "schedule next poll for chart build %s in %s remain=%d", args, interval, remain)
	if err := taskq.Enqueue(ctx, Task.NewTask(args), asynq.ProcessIn(interval)); err != nil {
		return errors.Wrap(err, "enqueue next chart build poll tick")
	}
	return nil
}

// terminate 把记录标为指定终态并落库，再走 onTerminated 副作用
func (m *Manager) terminate(
	ctx context.Context,
	record *helmchartbuild.Record,
	args Args,
	status helmchartbuild.Status,
) error {
	record.Status = status
	if record.EndedAt == nil {
		endedAt := time.Now()
		record.EndedAt = &endedAt
	}
	if err := m.save(ctx, record); err != nil {
		log.Errorf(ctx, "failed to update chart build record: %v", err)
	}
	return m.onTerminated(ctx, record, args)
}

// save 落 Chart 构建记录
func (m *Manager) save(ctx context.Context, record *helmchartbuild.Record) error {
	saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), saveStatusTimeout)
	defer cancel()
	return m.recordStore.Update(saveCtx, record)
}

// onTerminated 终态副作用失败只打日志，不改构建结果，也不让本 tick 失败
func (m *Manager) onTerminated(ctx context.Context, record *helmchartbuild.Record, args Args) error {
	opResult := lo.Ternary(
		record.Status == helmchartbuild.StatusSuccess,
		audit.ResultSuccess,
		audit.ResultFailed,
	)
	go audit.AddOperationRecordAsync(
		context.WithoutCancel(ctx), audit.OperationTypeBuild, audit.ResourceTypeApp, args.AppID,
		audit.WithResult(opResult), audit.WithAttribute(audit.AttributeHelmChart),
		audit.WithWorkspaceID(args.WorkspaceID), audit.WithAppID(args.AppID),
	)
	log.Infof(ctx, "chart build %s status is %s (Terminated), stop polling", args, record.Status)
	return nil
}

// fetchAndUpdateChartBuildRecord 查一次蓝盾并把状态、extras 写回 record。
// 抽成函数便于单测 mock；结束时间用 now，与现网 Chart 轮询一致。
func fetchAndUpdateChartBuildRecord(
	ctx context.Context,
	client bkciapi.Client,
	pipeline *bkci.Pipeline,
	record *helmchartbuild.Record,
	buildID string,
) error {
	buildState, err := client.GetPipelineBuildState(ctx, pipeline.ProjectCode, pipeline.ID, buildID)
	if err != nil {
		log.Errorf(
			ctx, "failed to get project %s pipeline %s chart build %s: %v",
			pipeline.ProjectCode, pipeline.ID, buildID, err,
		)
		return err
	}

	buildStatus := bkciapi.PipelineBuildStatus(buildState.Status)
	switch {
	case buildStatus.IsSuccess():
		record.Status = helmchartbuild.StatusSuccess
	case buildStatus.IsFailure():
		record.Status = helmchartbuild.StatusFailed
	case buildStatus.IsCancel():
		record.Status = helmchartbuild.StatusCanceled
	case buildStatus.IsRunning():
		record.Status = helmchartbuild.StatusRunning
	}
	if buildStatus.IsFinished() {
		endedAt := time.Now()
		record.EndedAt = &endedAt
	}
	record.Extras = collectHelmBuildExtras(buildState.Variables)
	return nil
}

// collectHelmBuildExtras 从蓝盾 Variables 中提取 Chart 构建关心的额外信息
// 仅取常用字段，不存在时填空字符串以保证字段稳定
func collectHelmBuildExtras(variables map[string]string) map[string]string {
	extras := make(map[string]string, len(pipelinevar.RequiredVariables))
	for _, n := range pipelinevar.RequiredVariables {
		extras[n] = variables[n]
	}
	return extras
}
