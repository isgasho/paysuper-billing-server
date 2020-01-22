package service

import (
	"context"
	"github.com/paysuper/paysuper-proto/go/billingpb"
	"sort"
)

func (s *Service) GetPlatforms(ctx context.Context, req *billingpb.ListPlatformsRequest, rsp *billingpb.ListPlatformsResponse) error {
	var platforms []*billingpb.Platform
	rsp.Status = billingpb.ResponseStatusOk
	var i int32
	for _, pl := range availablePlatforms {
		if i < req.Offset {
			i++
			continue
		}

		platforms = append(platforms, pl)
		if int32(len(platforms)) >= req.Limit {
			break
		}
		i++
	}

	sort.Slice(platforms, func(i, j int) bool {
		return platforms[i].Order < platforms[j].Order
	})

	rsp.Platforms = platforms
	rsp.Status = billingpb.ResponseStatusOk

	return nil
}
