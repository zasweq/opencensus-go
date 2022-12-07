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

package ocgrpc

import (
	"context"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"

	"go.opencensus.io/trace"
	"go.opencensus.io/trace/propagation"
)

// is the current system sufficient to handle this?

// interceptor adds top level id to collect all this data? Also retry metrics...
// thus, are we sure we even need an interceptor?

// call this at overall RPC level to create wrapper span

// then call this at retry level to create the child spans

// how is this parent span (child span child span) represented as go structs in this repository?

// How does the concept of a tag map to this repo? These child spans which map
// to call attempts have tags attached to them - previous-rpc-attempts and also
// transparent-retry

// Also
// (gRPC) -> (calls this plugin library)

// also scaling up -> to be an interceptor
// this -> is the stats handler interface, I don't think we want to change anything about the API


// overall rpc ->? this library using stats handler interface - to get the parent span (and also

// 			can do the thing with a timestamp (tag?), and the diff between timestamp for the metric of no attempt on the RPC
// 			how to get parent span - we can either do this with an interceptor or doug mentioned you can do this not technically required an interceptor

// ( overall rpc
// cs attempt -> this library using stats handler interface
// cs attempt -> this library using stats handler interface
// cs attempt -> this library using stats handler interface
// ) overall rpc

// all this upstream logic in gRPC that triggers the calls to this plugin
// library need to learn that, how does this logical layering of RPC on top of
// RPC Attempts map to the grpc-go system?

const traceContextKey = "grpc-trace-bin"

// newCsStream (the concept of a logical RPC) needs to call into this package, another callout
// cs.ctx will get dervived from to create the attempts context
// into traceTagRPC(cs.ctx, /*what RPCTagInfo do I call this with?*/), or it's just a fixed struct not an interface

// TagRPC creates a new trace span for the client side of the RPC.
//
// It returns ctx with the new trace span added and a serialization of the
// SpanContext added to the outgoing gRPC metadata.
func (c *ClientHandler) traceTagRPC(ctx context.Context, rti *stats.RPCTagInfo) context.Context {
	name := strings.TrimPrefix(rti.FullMethodName, "/")
	name = strings.Replace(name, "/", ".", -1)
	ctx, span := trace.StartSpan(ctx, name, // can't you just keep this as is? Docstring for the function - "If there is no span in the context, creates a new trace and span."
		trace.WithSampler(c.StartOptions.Sampler),
		trace.WithSpanKind(trace.SpanKindClient)) // span is ended by traceHandleRPC
	traceContextBinary := propagation.Binary(span.SpanContext())
	return metadata.AppendToOutgoingContext(ctx, traceContextKey, string(traceContextBinary))
}

// *** are we rewriting server side too? I feel like attempts only happen client
// side...

// TagRPC creates a new trace span for the server side of the RPC.
//
// It checks the incoming gRPC metadata in ctx for a SpanContext, and if
// it finds one, uses that SpanContext as the parent context of the new span.
//
// It returns ctx, with the new trace span added.
func (s *ServerHandler) traceTagRPC(ctx context.Context, rti *stats.RPCTagInfo) context.Context {
	md, _ := metadata.FromIncomingContext(ctx)
	name := strings.TrimPrefix(rti.FullMethodName, "/")
	name = strings.Replace(name, "/", ".", -1)
	traceContext := md[traceContextKey]
	var (
		parent     trace.SpanContext
		haveParent bool
	)
	if len(traceContext) > 0 {
		// Metadata with keys ending in -bin are actually binary. They are base64
		// encoded before being put on the wire, see:
		// https://github.com/grpc/grpc-go/blob/08d6261/Documentation/grpc-metadata.md#storing-binary-data-in-metadata
		traceContextBinary := []byte(traceContext[0])
		parent, haveParent = propagation.FromBinary(traceContextBinary)
		if haveParent && !s.IsPublicEndpoint {
			ctx, _ := trace.StartSpanWithRemoteParent(ctx, name, parent,
				trace.WithSpanKind(trace.SpanKindServer),
				trace.WithSampler(s.StartOptions.Sampler),
			)
			return ctx
		}
	}
	ctx, span := trace.StartSpan(ctx, name,
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithSampler(s.StartOptions.Sampler))
	// A trace is a directed acyclic graph (DAG) of spans, where the edges between spans are called references.
	// Link represents a reference from one span to another span.
	/*
	const LinkTypeChild LinkType
	Resolved value:
	1
	The linked span is a child of the current span.
	*/

	// trace id is 16 bit identifier for a set of spans

	// span id is 8 bit identifier for a single span

	// how are the child and parent spans linked? I'm assuming a trace id...

	// overall rpc -> (can either be through interceptor or stats handler) create a parent span
	//     each attempt underneath is logically going to be tied to overall rpc no matter what
	//     child span with the same traceID?...to link, where is this traceID persisted in the system
	//     to pass down to child emitted events?


	// I also think this is all client side

	// logically, what is an rpc? - where is this in the system? argh, also have streaming and unary flow (is it different or same on client/server side?)
	// logically, what is an attempt? - where is this in the system?

	// follow unary flow downward
	//    see overall rpc
	//           rpc attempt
	//           rpc attempt

	// follow streaming flow downward
	//    see overall rpc
	//           rpc attempt (how does this relationship with overall even work, what granularity, seems like child?)
	//           rpc attempt

	// do both of these unary and streaming flow go to the same place logically?

	// go to the same place
	// client stream - kind of logically an RPC concept
	//       csAttempt - the actual attempt, the first attempt is still an attempt, if retry is disabled retryEnabled is simply false and it just doesn't retry
	//       csAttempt -

	// there's a lot more complication to the csAttempt such as caching etc. within that system


	// Document update go.mod after every branch cut, and also why (gives you 6
	// weeks to fix any breakages before next branch cut - compilation
	// errors/breakages in imports as a result)


	if haveParent {
		span.AddLink(trace.Link{TraceID: parent.TraceID, SpanID: parent.SpanID, Type: trace.LinkTypeChild})
	}
	return ctx
}

// These metrics are on default gRPC flow

// is it configure (how to turn opencensus on?) and you get both interceptor and
// stats handler ("they need to work in conjunction"), I think so...

func traceTagRPCNowWithTopLevelCallPossible(ctx context.Context, rti *stats.RPCTagInfo) context.Context {
	// or is top level interceptor guaranteed to be present?
}

// I thinnkkkkk it calls ^^^^ (every time new attempt lock), then calls vvvv

// desired behavior:
// first call (whether attempt or the whole rpc as a logical concept) - create the overall trace
// Any subsequent attempt - create a child trace of this overall trace

// what is behavior of system now (what I was triyng to figure out):

// how to get to what you want....

func traceHandleRPCNowWithTopLevelCallPossible(ctx context.Context, rti *stats.RPCTagInfo) {
	// This flow all stays the same right? Except you now have an additional top
	// level call and also you need to add the attributes (and get the data
	// somehow to attach to attributes)

	// I have the syntax down below for the attributes, where to plugin to
	// existing switch statement? and how do you actually determine the data to
	// attach to the attribute? (this is dependent on where you put it - what
	// data you have and how to get it there)




	// begin represents an RPC attempt right? maybe add it there and just read off data. Triggered by a begin{} HandleRPC call...
	pra := trace.Int64Attribute("previous-rpc-attempts", /*number of preceding attempts - transparent retry not included, how do we count this?*/)
	// Begin only has a bool that represents whether transparent retry
	// if !begin.IsTransparentRetry
	//       if not there set to 1
	//       if there read it and add 1, overwrite att essentially to att + 1 (<<< map these abstract ideas to implementation)

	/*
	AddAttributes sets attributes in the span.
	Existing attributes whose keys appear in the attributes parameter are overwritten.
	*/

	// persisting data in the attribute itself, not an object etc. I guess persisting it in the attribute which is a part of the object...





	// oh lol, begin.IsTransparentRetry is the data
	tr := trace.BoolAttribute("transparent-retry", /*data somehow plumbed into this function representing whether something is transparent retry, does this through the api passed in?*/)

	// I'm assuming this span end() is the same time, call this function with stats.End(), yeah stats.End

}

// I THINKKKK THAT this can just be left alone...other than the addition of the span attributes...
func traceHandleRPC(ctx context.Context, rs stats.RPCStats) {
	// trace.Span, you're not changing the way that's represented, you have
	// control of this plugin, so learning this data in trace package that is
	// fixed and I won't scale it up, struct should support all the logical
	// concepts mentioned in design
	span := trace.FromContext(ctx) // just adds stuff to the already created Span...
	// TODO: compressed and uncompressed sizes are not populated in every message.
	switch rs := rs.(type) {
	case *stats.Begin: // ohhhhh, it's using an old version of gRPC, but this won't be a problem if you move it to gRPC module
		// Begin only has a bool that represents whether transparent retry
		// /*number of preceding attempts - transparent retry not included, how do we count this?*/ wait even if this is zero it unconditionally writes...

		// "For each call, there should be some way to attach an object that
		// will gather data for individual call attempts. Events on the
		// underlying call attempts will be recorded via that object."?
		// Wait, this is the same thing as
		// grpc.io/client/retries_per_call	1	Number of retry or hedging
		// attempts excluding transparent retries made during the client call.
		// The original attempt is not counted as a retry/hedging attempt. Can't
		// you reuse this object in tracing?






		// if !begin.IsTransparentRetry (Does this algorithm need synchronization considerations due to hedging?)
		//       if not there set to 1
		//       if there read it and add 1, overwrite att essentially to att + 1 (<<< map these abstract ideas to implementation)


		// or just instead of set to x, write to a local var x and then call, creating a new one or overwriting what is already there with x

		// calculatedX
		// scenario                             want number calculated - number of preceding attempts - transparent retry not included
		// first csAttempt (non-transparent)     0 (attached to the end) (reads nothing? then set 0)
		// second csAttempt (non-transparent)    1 (attached to the end) (reads 0, then set 0 + 1)
		// third csAttempt (transparent)         1 (attached to the end) (reads 1, then set 1 + 1), but then the next read needs to not count

		// if previous-attempts not set
		//      set 0 regardless
		// if previous-attempts set
		//      read it
		//      if transparent
		//          no-op, stays as is
		//      else
		//          add 1 and rewrite variable
		// ^^^^^ no this is wrong lol


		/*
			AddAttributes sets attributes in the span.
			Existing attributes whose keys appear in the attributes parameter are overwritten.
		*/







	span.AddAttributes(
			trace.BoolAttribute("Client", rs.Client),
			trace.BoolAttribute("FailFast", rs.FailFast),
			trace.BoolAttribute("previous-rpc-attempts", calculatedX),
			trace.BoolAttribute("transparent-retry", rs.IsTransparentRetryAttempt)
			)
	case *stats.InPayload:
		span.AddMessageReceiveEvent(0 /* TODO: messageID */, int64(rs.Length), int64(rs.WireLength))
	case *stats.OutPayload:
		span.AddMessageSendEvent(0, int64(rs.Length), int64(rs.WireLength)) /// my child attempt spans need to record the message events (looks like both sends and receives) of the attempt. Already records sends and receives.
	case *stats.End:
		if rs.Error != nil {
			s, ok := status.FromError(rs.Error)
			if ok {
				span.SetStatus(trace.Status{Code: int32(s.Code()), Message: s.Message()})
			} else {
				span.SetStatus(trace.Status{Code: int32(codes.Internal), Message: rs.Error.Error()})
			}
		}
		span.End()
	case *stats./*X*/
	// does new client stream (a conceptual concept of an rpc) before retry, it itself as an operation call this arbitrarily?
	// what data does it pass in here the first pass
	// does a data type of stats already implement all the needed data
	// to determine this?


	// the retry attempt needs to know previous rpc attempts (read old span + 1)?
	// and also transparent retry, whether or not it's a transparent retry...

	}
}


// Some grounding:

// What is the logical trace object that we want to represent

// type out data structure

func traceWithChild() /*return a trace object with child - the thing that we want*/ {
	// Parent span (with way to link to child - I think i figured out (trace id
	// is 16 bit identifier for a set of spans)
	// Need to figure out how to plumb "Sent.<service name>.<method name>" into
	// the object

	// where is trace id generated? Completely random?

	/*
		StartSpan starts a new child span of the current span in the context. If
		there is no span in the context, creates a new trace and span. Returned
		context contains the newly created span. You can use it to propagate the
		returned span in process.
	*/
	// span is in the ctx? one span per context?
	// name = Sent.<service name>.<method name> service name and method name have to make it's way to this function somehow for this name and the name below
	ctx, span := trace.StartSpan(ctx, name /*, any options here?*/)
	// span.  // how to get traceID to create child spans, but if it does (create new trace + span) then (create span perhaps it does it internally for you)


	// i.e. set of spans so yeah link it with a trace id - does this happen in this package

	// 1. ^^^^ called once at the beginning of RPC, where is this logically called, "beginning of RPC" = ?


	// called an arbitrary amount of times as events, where an event is an RPC


	// current interface:
	// trace.HandleRPC (through stats handler) sh stats.RPCStats
	// trace.TagRPC *stats.RPCTagInfo (does this trigger the tag write on the trace object I think not)

	// ctx is the linkage? What data does this provide and where for you to produce this trace object?

	// the option is do we need to scale up the communication by adding our own interceptor

	// so this just has access to () all the interceptor data as is, then it has special logic?



	// 2. vvvv called when retried retried () () () (), need a counter for each of the events for previous-rpc-attempts


	// also, is this whole trace object scoped to the context 1:1? so service name method name at each function call,
	// if it is the context/that links together (the logical concept of this parent child child child span object with trace id)

	// plumb what around to each call? I think the context

	// how are these function calls plumbed?




	// the child spans linked need to have previous-rpc-attempts
	// and transparent-retry tagged to them
	var att trace.Attribute

	// an integer value, number of preceding attempts, transparent retry not included.
	// How do we count/represent this?
	pra := trace.Int64Attribute("previous-rpc-attempts", /*number of preceding attempts - transparent retry not included, how do we count this*/)
	// a boolean value, whether the attempt is a transparent retry
	tr := trace.BoolAttribute("transparent-retry", /*data somehow plumbed into this function representing whether something is transparent retry, do this through the api passed in?*/)

	span.AddAttributes(pra, tr) // here's how you get previous-rpc-attempts and transparent-retry attributes tagged

	// Need to figure out how to plumb "Attempt.<service name>.<method name>" into
	// the object

	// One option:

	/*	StartSpanWithRemoteParent starts a new child span of the span from the
		given parent. If the incoming context contains a parent, it ignores.
		StartSpanWithRemoteParent is preferred for cases where the parent is
		propagated via an incoming request. Returned context contains the newly
		created span. You can use it to propagate the returned span in process.*/

	trace. // looks like above is the only thing


}
