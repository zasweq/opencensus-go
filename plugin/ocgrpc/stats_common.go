// Copyright 2017, OpenCensus Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package ocgrpc

import (
	"context"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"go.opencensus.io/metric/metricdata"
	ocstats "go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	"go.opencensus.io/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

type grpcInstrumentationKey string

// rpcData holds the instrumentation RPC data that is needed between the start
// and end of an call. It holds the info that this package needs to keep track
// of between the various GRPC events.
type rpcData struct {
	// reqCount and respCount has to be the first words
	// in order to be 64-aligned on 32-bit architectures.
	sentCount, sentBytes, recvCount, recvBytes int64 // access atomically

	// startTime represents the time at which TagRPC was invoked at the
	// beginning of an RPC. It is an appoximation of the time when the
	// application code invoked GRPC code.
	startTime time.Time
	method    string
}

// The following variables define the default hard-coded auxiliary data used by
// both the default GRPC client and GRPC server metrics.
var (
	DefaultBytesDistribution        = view.Distribution(1024, 2048, 4096, 16384, 65536, 262144, 1048576, 4194304, 16777216, 67108864, 268435456, 1073741824, 4294967296)
	DefaultMillisecondsDistribution = view.Distribution(0.01, 0.05, 0.1, 0.3, 0.6, 0.8, 1, 2, 3, 4, 5, 6, 8, 10, 13, 16, 20, 25, 30, 40, 50, 65, 80, 100, 130, 160, 200, 250, 300, 400, 500, 650, 800, 1000, 2000, 5000, 10000, 20000, 50000, 100000)
	DefaultMessageCountDistribution = view.Distribution(1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536)
)

// Server tags are applied to the context used to process each RPC, as well as
// the measures at the end of each RPC.
var (
	KeyServerMethod = tag.MustNewKey("grpc_server_method")
	KeyServerStatus = tag.MustNewKey("grpc_server_status")
)

// Client tags are applied to measures at the end of each RPC.
var (
	KeyClientMethod = tag.MustNewKey("grpc_client_method")
	KeyClientStatus = tag.MustNewKey("grpc_client_status")
)

var (
	rpcDataKey = grpcInstrumentationKey("opencensus-rpcData")
)

func methodName(fullname string) string {
	return strings.TrimLeft(fullname, "/")
}

// statsHandleRPC processes the RPC events.
func statsHandleRPC(ctx context.Context, s stats.RPCStats) {
	switch st := s.(type) {
	case *stats.OutHeader, *stats.InHeader, *stats.InTrailer, *stats.OutTrailer:
		// do nothing for client
	case *stats.Begin:
		handleRPCBegin(ctx, st)
	case *stats.OutPayload:
		handleRPCOutPayload(ctx, st)
	case *stats.InPayload:
		handleRPCInPayload(ctx, st)
	case *stats.End:
		handleRPCEnd(ctx, st)
	default:
		grpclog.Infof("unexpected stats: %T", st)
	}
}

func handleRPCBegin(ctx context.Context, s *stats.Begin) {
	d, ok := ctx.Value(rpcDataKey).(*rpcData) // wait fuck this is already per RPC
	if !ok {
		if grpclog.V(2) {
			grpclog.Infoln("Failed to retrieve *rpcData from context.")
		}
	}

	// NO YOU CAN DO THIS AT THE BLOB

	// b := GetBlob(ctx)

	// iterate that object that's tied to RPC here...
	if s.IsTransparentRetryAttempt {
		// or do we want this to be int64 to keep consistent with previous rpc data?
		// I think that my idea is correct...it can't be negative and Doug won't notice that alligning thing in new PR?
		atomic.AddUint32(&b.countTransparentRetries, 1)
	} else {
		atomic.AddUint32(&b.countNonTransparentRetries, 1)
	}

	// that's it...the next question is timestamp

	// Is the downtime at beginning of RPC counted?

	// What are the operations passed that map to:

	// call start?

	/*
	(does the operation that is passed in have
	timestamp, can also take it here since the operations map 1:1 with these
	methods) they do have timestamps :D!
	*/
	// ( call attempt started

	// ) call attempt finished

	// does the beginning number happen (i.e. is there delay between call
	// starting and first attempt? Two seperate operations? Is there a way to
	// tell? Seems like no should start attempt immediately - due to backoff.)
	// "and the total amount of delay time caused by retry backoff."

	// if you do want the beginning, you could take a timestamp in the top level
	// interceptor's blob object thingy and the the delta to first attempt, but
	// would have to somehow know conditionally if it's the first attempt which
	// idk if this comlexity is worth it - counting retry backoff)

	// scenario:                    number you want:
	// 1 () 3 () 4 finish here...   8 - count the last interval as finished
	// 3 () 2 ( finish here         5 (don't count the last interval - could also implictly calculate this logically by counting the intervals between)
	//

	// what are we doing about timestamp, could also stick in rpcData?

	if s.IsClient() {
		ocstats.RecordWithOptions(ctx,
			ocstats.WithTags(tag.Upsert(KeyClientMethod, methodName(d.method))),
			ocstats.WithMeasurements(ClientStartedRPCs.M(1)))
	} else {
		ocstats.RecordWithOptions(ctx,
			ocstats.WithTags(tag.Upsert(KeyClientMethod, methodName(d.method))),
			ocstats.WithMeasurements(ServerStartedRPCs.M(1)))
	}
}

func handleRPCOutPayload(ctx context.Context, s *stats.OutPayload) {
	// rpcData - "for a call", logically coupled to call which wraps callAttempts
	// do you need to do anything past stick this rpcData in somewhere? or just scale up rpcData
	d, ok := ctx.Value(rpcDataKey).(*rpcData)
	if !ok {
		if grpclog.V(2) {
			grpclog.Infoln("Failed to retrieve *rpcData from context.")
		}
		return
	}

	atomic.AddInt64(&d.sentBytes, int64(s.Length))
	atomic.AddInt64(&d.sentCount, 1) // sentCount addition 1, number of sends
}

func handleRPCInPayload(ctx context.Context, s *stats.InPayload) {
	d, ok := ctx.Value(rpcDataKey).(*rpcData)
	if !ok {
		if grpclog.V(2) {
			grpclog.Infoln("Failed to retrieve *rpcData from context.")
		}
		return
	}

	atomic.AddInt64(&d.recvBytes, int64(s.Length))
	atomic.AddInt64(&d.recvCount, 1)
}

func handleRPCEnd(ctx context.Context, s *stats.End) {
	d, ok := ctx.Value(rpcDataKey).(*rpcData)
	if !ok {
		if grpclog.V(2) {
			grpclog.Infoln("Failed to retrieve *rpcData from context.")
		}
		return
	}

	// Can this only get called once per call? (rpc level)
	// if so, you can load the per call data here and emit the metrics then
	// I think time spent without an active attempt can also be emitted at the end of the call.
	// but the discrete intervals calculated are still somewhat complicated

	// load per call information here - that blob object thingy, stick it in the
	// measurement recordings at the end (so these current ones map per attempt
	// -> distribution, how do you do it per call, does it happen implicitly or
	// explicitly?)


	// Feng, Doug, Easwar, Eric as my peer reviewers



	elapsedTime := time.Since(d.startTime)

	var st string
	if s.Error != nil {
		s, ok := status.FromError(s.Error)
		if ok {
			st = statusCodeToString(s)
		}
	} else {
		st = "OK"
	}

	latencyMillis := float64(elapsedTime) / float64(time.Millisecond)
	attachments := getSpanCtxAttachment(ctx)
	if s.Client {
		ocstats.RecordWithOptions(ctx,
			ocstats.WithTags(
				tag.Upsert(KeyClientMethod, methodName(d.method)),
				tag.Upsert(KeyClientStatus, st)),
			ocstats.WithAttachments(attachments),
			ocstats.WithMeasurements(
				ClientSentBytesPerRPC.M(atomic.LoadInt64(&d.sentBytes)),
				ClientSentMessagesPerRPC.M(atomic.LoadInt64(&d.sentCount)), // meaured here at RPC end, so this is NOT steady stream, happens once, I'm assuming this already partions per RPC. Wait this is per attempt not per rpc and happens at the end. If you have the object in the top level context, will make it per call not per attempt
				ClientReceivedMessagesPerRPC.M(atomic.LoadInt64(&d.recvCount)),
				ClientReceivedBytesPerRPC.M(atomic.LoadInt64(&d.recvBytes)),
				ClientRoundtripLatency.M(latencyMillis)),
				// Above is per attempt ^^^ (for the view distribution and sum)

				// This is correct imo, vvv is per call...only question is how to do this for distribution and sum for the /call vs. /attempt ^^^ measurement)

				// ClientRetriesPerCall.M(b.countNonTransparentRetries)) (should we rename the perRPC to per Attempt since I own this now?)
				// ClientTransparentRetriesPerCall.M(b.countTransparentRetries))
				// ClientRetryDelayPerCall.M(b.timeNoActiveAttempt))
		)
	} else {
		ocstats.RecordWithOptions(ctx,
			ocstats.WithTags(
				tag.Upsert(KeyServerStatus, st),
			),
			ocstats.WithAttachments(attachments),
			ocstats.WithMeasurements(
				ServerSentBytesPerRPC.M(atomic.LoadInt64(&d.sentBytes)),
				ServerSentMessagesPerRPC.M(atomic.LoadInt64(&d.sentCount)),
				ServerReceivedMessagesPerRPC.M(atomic.LoadInt64(&d.recvCount)),
				ServerReceivedBytesPerRPC.M(atomic.LoadInt64(&d.recvBytes)),
				ServerLatency.M(latencyMillis)))
	}
}

func statusCodeToString(s *status.Status) string {
	// see https://github.com/grpc/grpc/blob/master/doc/statuscodes.md
	switch c := s.Code(); c {
	case codes.OK:
		return "OK"
	case codes.Canceled:
		return "CANCELLED"
	case codes.Unknown:
		return "UNKNOWN"
	case codes.InvalidArgument:
		return "INVALID_ARGUMENT"
	case codes.DeadlineExceeded:
		return "DEADLINE_EXCEEDED"
	case codes.NotFound:
		return "NOT_FOUND"
	case codes.AlreadyExists:
		return "ALREADY_EXISTS"
	case codes.PermissionDenied:
		return "PERMISSION_DENIED"
	case codes.ResourceExhausted:
		return "RESOURCE_EXHAUSTED"
	case codes.FailedPrecondition:
		return "FAILED_PRECONDITION"
	case codes.Aborted:
		return "ABORTED"
	case codes.OutOfRange:
		return "OUT_OF_RANGE"
	case codes.Unimplemented:
		return "UNIMPLEMENTED"
	case codes.Internal:
		return "INTERNAL"
	case codes.Unavailable:
		return "UNAVAILABLE"
	case codes.DataLoss:
		return "DATA_LOSS"
	case codes.Unauthenticated:
		return "UNAUTHENTICATED"
	default:
		return "CODE_" + strconv.FormatInt(int64(c), 10)
	}
}

func getSpanCtxAttachment(ctx context.Context) metricdata.Attachments {
	attachments := map[string]interface{}{}
	span := trace.FromContext(ctx) // weird, interfaces with trace here...if available
	if span == nil {
		return attachments
	}
	spanCtx := span.SpanContext()
	if spanCtx.IsSampled() {
		attachments[metricdata.AttachmentKeySpanContext] = spanCtx
	}
	return attachments
}
