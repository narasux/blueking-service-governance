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
	"errors"
	"time"

	"github.com/bytedance/mockey"
	"github.com/hibiken/asynq"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.mongodb.org/mongo-driver/v2/bson"
	helmrelease "helm.sh/helm/v3/pkg/release"

	helmdeploy "github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/deploy/helm"
	"github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/infras/helm"
	"github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/infras/taskq"
	"github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/misc/audit"
)

func newPendingRecord() *helmdeploy.Record {
	return &helmdeploy.Record{
		ID:          bson.NewObjectID(),
		AppID:       "app-1",
		EnvName:     "dev",
		ReleaseName: "dev-app-1",
		Status:      helm.StatusPendingUpgrade,
		StartedAt:   time.Now(),
		Operator:    "alice",
	}
}

func stubFetch(status helmrelease.Status, revision string) {
	mockey.Mock(fetchReleaseStatus).Return(
		&helm.Release{
			Version: revision,
			DeployResult: helm.DeployResult{
				Status:      status,
				Description: string(status),
			},
		},
		nil,
	).Build()
}

var _ = Describe("Manager Handle", func() {
	var (
		ctx        context.Context
		args       Args
		rec        *helmdeploy.Record
		latest     *helmdeploy.Record
		enqCount   int
		releaseCnt int
		mgr        *Manager
	)

	BeforeEach(func() {
		ctx = context.Background()
		rec = newPendingRecord()
		latest = rec
		args = Args{WorkspaceID: "ws-1", AppID: "app-1", EnvName: "dev", DeployID: rec.ID.Hex()}
		enqCount = 0
		releaseCnt = 0
		mgr = NewManager(&helmdeploy.RecordStoreMongo{})

		mockey.Mock((*helmdeploy.RecordStoreMongo).Get).Return(rec, nil).Build()
		mockey.Mock((*helmdeploy.RecordStoreMongo).GetLatest).To(
			func(context.Context, string, string, string) (*helmdeploy.Record, error) { return latest, nil },
		).Build()
		mockey.Mock((*helmdeploy.RecordStoreMongo).Update).Return(nil).Build()
		mockey.Mock(taskq.Enqueue).To(func(context.Context, *taskq.Task, ...asynq.Option) error {
			enqCount++
			return nil
		}).Build()
		mockey.Mock(audit.AddOperationRecordAsync).Return().Build()
		mockey.Mock(triggerTopologyRefresh).Return().Build()
		mockey.Mock(handleDeploySucceeded).Return().Build()
		mockey.Mock(releaseDeployLock).To(func(context.Context, Args) { releaseCnt++ }).Build()
	})

	AfterEach(func() {
		time.Sleep(50 * time.Millisecond)
		mockey.UnPatchAll()
	})

	It("enqueues the next tick when deploy is still running", func() {
		stubFetch(helm.StatusPendingUpgrade, "2")
		err := mgr.Handle(ctx, args)
		Expect(err).NotTo(HaveOccurred())
		Expect(enqCount).To(Equal(1))
		Expect(releaseCnt).To(Equal(0))
		Expect(rec.Status).To(Equal(helm.StatusPendingUpgrade))
	})

	It("skips already stable records without polling", func() {
		rec.Status = helm.StatusDeployed
		err := mgr.Handle(ctx, args)
		Expect(err).NotTo(HaveOccurred())
		Expect(enqCount).To(Equal(0))
		Expect(releaseCnt).To(Equal(1))
	})

	It("does not release lock when a newer deploy is latest", func() {
		rec.Status = helm.StatusDeployed
		latest = newPendingRecord()
		err := mgr.Handle(ctx, args)
		Expect(err).NotTo(HaveOccurred())
		Expect(enqCount).To(Equal(0))
		Expect(releaseCnt).To(Equal(0))
	})

	It("marks pollingTimeout when StartedAt exceeds configured window", func() {
		rec.StartedAt = time.Now().Add(-pollingTimeout() - time.Minute)
		err := mgr.Handle(ctx, args)
		Expect(err).NotTo(HaveOccurred())
		Expect(rec.Status).To(Equal(helm.StatusPollingTimeout))
		Expect(enqCount).To(Equal(0))
		Expect(releaseCnt).To(Equal(1))
	})

	It("marks pollingBroken after remaining retries are exhausted", func() {
		args.FailureRetryRemain = 1
		mockey.Mock(fetchReleaseStatus).Return(nil, errors.New("cluster down")).Build()
		err := mgr.Handle(ctx, args)
		Expect(err).NotTo(HaveOccurred())
		Expect(rec.Status).To(Equal(helm.StatusPollingBroken))
		Expect(enqCount).To(Equal(0))
		Expect(releaseCnt).To(Equal(1))
	})

	It("reschedules when query fails but retries remain", func() {
		args.FailureRetryRemain = 3
		mockey.Mock(fetchReleaseStatus).Return(nil, errors.New("cluster down")).Build()
		err := mgr.Handle(ctx, args)
		Expect(err).NotTo(HaveOccurred())
		Expect(rec.Status).To(Equal(helm.StatusPendingUpgrade))
		Expect(enqCount).To(Equal(1))
		Expect(releaseCnt).To(Equal(0))
	})

	It("writes revision and stops when status becomes deployed", func() {
		stubFetch(helm.StatusDeployed, "5")
		err := mgr.Handle(ctx, args)
		Expect(err).NotTo(HaveOccurred())
		Expect(rec.Status).To(Equal(helm.StatusDeployed))
		Expect(rec.Revision).To(Equal("5"))
		Expect(rec.Message).To(Equal(string(helm.StatusDeployed)))
		Expect(enqCount).To(Equal(0))
		Expect(releaseCnt).To(Equal(1))
	})
})
