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
	"errors"
	"time"

	"github.com/bytedance/mockey"
	"github.com/hibiken/asynq"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/bkintegrations/bkci"
	helmchartbuild "github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/build/chart"
	"github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/infras/account/auth"
	bkciapi "github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/infras/cloudapi/bkci"
	"github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/infras/taskq"
	"github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/misc/audit"
)

func newRunningRecord() *helmchartbuild.Record {
	return &helmchartbuild.Record{
		AppID:     "app-1",
		BuildID:   "build-1",
		Status:    helmchartbuild.StatusRunning,
		StartedAt: time.Now(),
		Params:    map[string]string{},
	}
}

func stubFetch(status helmchartbuild.Status) {
	mockey.Mock(fetchAndUpdateChartBuildRecord).To(
		func(
			_ context.Context,
			_ bkciapi.Client,
			_ *bkci.Pipeline,
			record *helmchartbuild.Record,
			_ string,
		) error {
			record.Status = status
			if record.IsTerminated() {
				endedAt := time.Now()
				record.EndedAt = &endedAt
			}
			return nil
		},
	).Build()
}

var _ = Describe("Manager Handle", func() {
	var (
		ctx      context.Context
		args     Args
		rec      *helmchartbuild.Record
		enqCount int
		enqDelay time.Duration
		mgr      *Manager
	)

	BeforeEach(func() {
		ctx = auth.WithUser(context.Background(), auth.User{ID: "alice"})
		args = Args{WorkspaceID: "ws-1", AppID: "app-1", BuildID: "build-1"}
		rec = newRunningRecord()
		enqCount = 0
		enqDelay = 0
		mgr = NewManager(&helmchartbuild.RecordStoreMongo{}, &bkci.PipelineStoreMongo{})

		mockey.Mock((*helmchartbuild.RecordStoreMongo).Get).To(
			func(_ context.Context, appID, buildID string) (*helmchartbuild.Record, error) {
				if rec == nil || rec.AppID != appID || rec.BuildID != buildID {
					return nil, errors.New("chart build record not found")
				}
				cp := *rec
				return &cp, nil
			},
		).Build()
		mockey.Mock((*helmchartbuild.RecordStoreMongo).Update).To(
			func(_ context.Context, record *helmchartbuild.Record) error {
				cp := *record
				*rec = cp
				return nil
			},
		).Build()
		mockey.Mock((*bkci.PipelineStoreMongo).GetByWorkspaceAndType).Return(&bkci.Pipeline{
			ID: "p-1", ProjectCode: "proj", Type: string(bkci.PipelineTypeHelmGitBuild), WorkspaceID: "ws-1",
		}, nil).Build()
		mockey.Mock(bkciapi.New).Return(nil, nil).Build()
		mockey.Mock(taskq.Enqueue).To(func(_ context.Context, _ *taskq.Task, opts ...asynq.Option) error {
			enqCount++
			for _, opt := range opts {
				if opt.Type() == asynq.ProcessInOpt {
					enqDelay = opt.Value().(time.Duration)
				}
			}
			return nil
		}).Build()
		mockey.Mock(audit.AddOperationRecordAsync).Return().Build()
	})

	AfterEach(func() {
		time.Sleep(50 * time.Millisecond)
		mockey.UnPatchAll()
	})

	It("enqueues the next tick with ProcessIn when build is still running", func() {
		stubFetch(helmchartbuild.StatusRunning)
		err := mgr.Handle(ctx, args)
		Expect(err).NotTo(HaveOccurred())
		Expect(enqCount).To(Equal(1))
		Expect(enqDelay).To(Equal(5 * time.Second))
		Expect(rec.Status).To(Equal(helmchartbuild.StatusRunning))
	})

	It("skips already terminated records without polling", func() {
		rec.Status = helmchartbuild.StatusSuccess
		err := mgr.Handle(ctx, args)
		Expect(err).NotTo(HaveOccurred())
		Expect(enqCount).To(Equal(0))
	})

	It("marks pollingTimeout when StartedAt exceeds 10min", func() {
		rec.StartedAt = time.Now().Add(-11 * time.Minute)
		err := mgr.Handle(ctx, args)
		Expect(err).NotTo(HaveOccurred())
		Expect(rec.Status).To(Equal(helmchartbuild.StatusPollingTimeout))
		Expect(enqCount).To(Equal(0))
	})

	It("marks pollingBroken after remaining retries are exhausted", func() {
		args.FailureRetryRemain = 1
		mockey.Mock(fetchAndUpdateChartBuildRecord).Return(errors.New("bkci down")).Build()
		err := mgr.Handle(ctx, args)
		Expect(err).NotTo(HaveOccurred())
		Expect(rec.Status).To(Equal(helmchartbuild.StatusPollingBroken))
		Expect(enqCount).To(Equal(0))
	})

	It("reschedules when bkci fails but retries remain", func() {
		args.FailureRetryRemain = 3
		mockey.Mock(fetchAndUpdateChartBuildRecord).Return(errors.New("bkci down")).Build()
		err := mgr.Handle(ctx, args)
		Expect(err).NotTo(HaveOccurred())
		Expect(rec.Status).To(Equal(helmchartbuild.StatusRunning))
		Expect(enqCount).To(Equal(1))
	})

	It("writes success and stops polling", func() {
		stubFetch(helmchartbuild.StatusSuccess)
		err := mgr.Handle(ctx, args)
		Expect(err).NotTo(HaveOccurred())
		Expect(rec.Status).To(Equal(helmchartbuild.StatusSuccess))
		Expect(enqCount).To(Equal(0))
	})
})
