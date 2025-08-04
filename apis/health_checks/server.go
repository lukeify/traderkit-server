package apis_health_checks

import (
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type HealthChecksServerImpl struct {
	UnimplementedHealthChecksServer
}

func (hcs *HealthChecksServerImpl) Monitor(_ *emptypb.Empty, stream grpc.ServerStreamingServer[HealthCheckMonitorResponse]) error {
	err := stream.Send(&HealthCheckMonitorResponse{Ok: true})
	return err
}
