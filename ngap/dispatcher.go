// SPDX-FileCopyrightText: 2022-present Intel Corporation
// SPDX-FileCopyrightText: 2021 Open Networking Foundation <info@opennetworking.org>
// Copyright 2019 free5GC.org
//
// SPDX-License-Identifier: Apache-2.0
//

package ngap

import (
	ctxt "context"
	"fmt"
	"net"
	"os"
	"reflect"

	"git.cs.nctu.edu.tw/calee/sctp"
	"github.com/omec-project/amf/context"
	"github.com/omec-project/amf/logger"
	"github.com/omec-project/amf/metrics"
	"github.com/omec-project/amf/msgtypes/ngapmsgtypes"
	"github.com/omec-project/amf/protos/sdcoreAmfServer"
	"github.com/omec-project/ngap"
	"github.com/omec-project/ngap/ngapType"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var tracer = otel.Tracer("amf/ngap")

type ngapHandlerFunc func(ctx ctxt.Context, ran *context.AmfRan, message *ngapType.NGAPPDU, sctplbMsg *sdcoreAmfServer.SctplbMessage)

var (
	initiatingMessageHandlers = map[int64]ngapHandlerFunc{
		ngapType.ProcedureCodeNGSetup:                             HandleNGSetupRequest,
		ngapType.ProcedureCodeInitialUEMessage:                    HandleInitialUEMessage,
		ngapType.ProcedureCodeUplinkNASTransport:                  HandleUplinkNasTransport,
		ngapType.ProcedureCodeNGReset:                             HandleNGReset,
		ngapType.ProcedureCodeHandoverCancel:                      HandleHandoverCancel,
		ngapType.ProcedureCodeUEContextReleaseRequest:             HandleUEContextReleaseRequest,
		ngapType.ProcedureCodeNASNonDeliveryIndication:            HandleNasNonDeliveryIndication,
		ngapType.ProcedureCodeLocationReportingFailureIndication:  HandleLocationReportingFailureIndication,
		ngapType.ProcedureCodeErrorIndication:                     HandleErrorIndication,
		ngapType.ProcedureCodeUERadioCapabilityInfoIndication:     HandleUERadioCapabilityInfoIndication,
		ngapType.ProcedureCodeHandoverNotification:                HandleHandoverNotify,
		ngapType.ProcedureCodeHandoverPreparation:                 HandleHandoverRequired,
		ngapType.ProcedureCodeRANConfigurationUpdate:              HandleRanConfigurationUpdate,
		ngapType.ProcedureCodeRRCInactiveTransitionReport:         HandleRRCInactiveTransitionReport,
		ngapType.ProcedureCodePDUSessionResourceNotify:            HandlePDUSessionResourceNotify,
		ngapType.ProcedureCodePathSwitchRequest:                   HandlePathSwitchRequest,
		ngapType.ProcedureCodeLocationReport:                      HandleLocationReport,
		ngapType.ProcedureCodeUplinkUEAssociatedNRPPaTransport:    HandleUplinkUEAssociatedNRPPATransport,
		ngapType.ProcedureCodeUplinkRANConfigurationTransfer:      HandleUplinkRanConfigurationTransfer,
		ngapType.ProcedureCodePDUSessionResourceModifyIndication:  HandlePDUSessionResourceModifyIndication,
		ngapType.ProcedureCodeCellTrafficTrace:                    HandleCellTrafficTrace,
		ngapType.ProcedureCodeUplinkRANStatusTransfer:             HandleUplinkRanStatusTransfer,
		ngapType.ProcedureCodeUplinkNonUEAssociatedNRPPaTransport: HandleUplinkNonUEAssociatedNRPPATransport,
	}

	successfulOutcomeHandlers = map[int64]ngapHandlerFunc{
		ngapType.ProcedureCodeNGReset:                    HandleNGResetAcknowledge,
		ngapType.ProcedureCodeUEContextRelease:           HandleUEContextReleaseComplete,
		ngapType.ProcedureCodePDUSessionResourceRelease:  HandlePDUSessionResourceReleaseResponse,
		ngapType.ProcedureCodeUERadioCapabilityCheck:     HandleUERadioCapabilityCheckResponse,
		ngapType.ProcedureCodeAMFConfigurationUpdate:     HandleAMFconfigurationUpdateAcknowledge,
		ngapType.ProcedureCodeInitialContextSetup:        HandleInitialContextSetupResponse,
		ngapType.ProcedureCodeUEContextModification:      HandleUEContextModificationResponse,
		ngapType.ProcedureCodePDUSessionResourceSetup:    HandlePDUSessionResourceSetupResponse,
		ngapType.ProcedureCodePDUSessionResourceModify:   HandlePDUSessionResourceModifyResponse,
		ngapType.ProcedureCodeHandoverResourceAllocation: HandleHandoverRequestAcknowledge,
	}

	unsuccessfulOutcomeHandlers = map[int64]ngapHandlerFunc{
		ngapType.ProcedureCodeAMFConfigurationUpdate:     HandleAMFconfigurationUpdateFailure,
		ngapType.ProcedureCodeInitialContextSetup:        HandleInitialContextSetupFailure,
		ngapType.ProcedureCodeUEContextModification:      HandleUEContextModificationFailure,
		ngapType.ProcedureCodeHandoverResourceAllocation: HandleHandoverFailure,
	}
)

func DispatchLb(ctx ctxt.Context, sctplbMsg *sdcoreAmfServer.SctplbMessage, Amf2RanMsgChan chan *sdcoreAmfServer.AmfMessage) {
	logger.NgapLog.Infof("dispatchLb GnbId:%v GnbIp: %v %T", sctplbMsg.GnbId, sctplbMsg.GnbIpAddr, Amf2RanMsgChan)
	var ran *context.AmfRan
	amfSelf := context.AMF_Self()

	if sctplbMsg.GnbId != "" {
		var ok bool
		ran, ok = amfSelf.AmfRanFindByGnbId(sctplbMsg.GnbId)
		if !ok {
			logger.NgapLog.Infof("create a new NG connection for: %s", sctplbMsg.GnbId)
			ran = amfSelf.NewAmfRanId(sctplbMsg.GnbId)
			ran.Amf2RanMsgChan = Amf2RanMsgChan
			logger.NgapLog.Infof("dispatchLb, Create new Amf RAN", sctplbMsg.GnbId)
		}
	} else if sctplbMsg.GnbIpAddr != "" {
		logger.NgapLog.Infoln("GnbIpAddress received but no GnbId")
		ran = &context.AmfRan{}
		ran.SupportedTAList = context.NewSupportedTAIList()
		ran.Amf2RanMsgChan = Amf2RanMsgChan
		ran.Log = logger.NgapLog.With(logger.FieldRanAddr, sctplbMsg.GnbIpAddr)
		ran.GnbIp = sctplbMsg.GnbIpAddr
		logger.NgapLog.Infoln("dispatchLb, Create new Amf RAN with GnbIpAddress", sctplbMsg.GnbIpAddr)
	}

	if len(sctplbMsg.Msg) == 0 {
		logger.NgapLog.Infof("dispatchLb, Message of size 0 - ", sctplbMsg.GnbId)
		ran.Log.Infoln("RAN close the connection")
		ran.Remove()
		return
	}

	pdu, err := ngap.Decoder(sctplbMsg.Msg)
	if err != nil {
		ran.Log.Errorf("NGAP decode error: %+v", err)
		logger.NgapLog.Infoln("dispatchLb, decode Messgae error", sctplbMsg.GnbId)
		return
	}

	ranUe, ngapId := FetchRanUeContext(ran, pdu)
	if ngapId != nil {
		//ranUe.Log.Debugln("RanUe RanNgapId AmfNgapId: ", ranUe.RanUeNgapId, ranUe.AmfUeNgapId)
		/* checking whether same AMF instance can handle this message */
		/* redirect it to correct owner if required */
		if amfSelf.EnableDbStore {
			id, err := amfSelf.Drsm.FindOwnerInt32ID(int32(ngapId.Value))
			if id == nil || err != nil {
				ran.Log.Warnf("dispatchLb, Couldn't find owner for amfUeNgapid: %v", ngapId.Value)
			} else if id.PodName != os.Getenv("HOSTNAME") {
				rsp := &sdcoreAmfServer.AmfMessage{}
				rsp.VerboseMsg = "Redirect Msg From AMF Pod !"
				rsp.Msgtype = sdcoreAmfServer.MsgType_REDIRECT_MSG
				rsp.AmfId = os.Getenv("HOSTNAME")
				/* TODO set only pod name, for this release setting pod ip to simplify logic in sctplb */
				logger.NgapLog.Infof("dispatchLb, amfNgapId: %v is not for this amf instance, redirect to amf instance: %v %v", ngapId.Value, id.PodName, id.PodIp)
				rsp.RedirectId = id.PodIp
				rsp.GnbId = ran.GnbId
				rsp.Msg = make([]byte, len(sctplbMsg.Msg))
				copy(rsp.Msg, sctplbMsg.Msg)
				ran.Amf2RanMsgChan = Amf2RanMsgChan
				ran.Amf2RanMsgChan <- rsp
				if ranUe != nil && ranUe.AmfUe != nil {
					ranUe.AmfUe.Remove()
				}
				if ranUe != nil {
					if err := ranUe.Remove(); err != nil {
						ran.Log.Errorf("could not remove ranUe: %v", err)
					}
				}
				return
			} else {
				ran.Log.Debugf("DispatchLb, amfNgapId: %v for this amf instance", ngapId.Value)
			}
		}
	}

	/* uecontext is found, submit the message to transaction queue*/
	if ranUe != nil && ranUe.AmfUe != nil {
		ranUe.AmfUe.SetEventChannel(ctx, NgapMsgHandler)
		// ranUe.AmfUe.TxLog.Infoln("Uecontext found. queuing ngap message to uechannel")
		ranUe.AmfUe.EventChannel.UpdateNgapHandler(NgapMsgHandler)
		ngapMsg := context.NgapMsg{
			Ran:       ran,
			NgapMsg:   pdu,
			SctplbMsg: sctplbMsg,
		}
		ranUe.AmfUe.EventChannel.SubmitMessage(ngapMsg)
		return
	}
	go DispatchNgapMsg(ctx, ran, pdu, sctplbMsg)
}

func Dispatch(conn net.Conn, msg []byte) {
	var ran *context.AmfRan
	amfSelf := context.AMF_Self()

	ctx := ctxt.Background()

	ran, ok := amfSelf.AmfRanFindByConn(conn)
	if !ok {
		logger.NgapLog.Infof("Create a new NG connection for: %s", conn.RemoteAddr().String())
		ran = amfSelf.NewAmfRan(conn)
	}

	if len(msg) == 0 {
		ran.Log.Infoln("RAN close the connection")
		ran.Remove()
		return
	}

	pdu, err := ngap.Decoder(msg)
	if err != nil {
		ran.Log.Errorf("NGAP decode error: %+v", err)
		return
	}

	ranUe, _ := FetchRanUeContext(ran, pdu)

	/* uecontext is found, submit the message to transaction queue*/
	if ranUe != nil && ranUe.AmfUe != nil {
		ranUe.AmfUe.SetEventChannel(ctx, NgapMsgHandler)
		ranUe.AmfUe.TxLog.Infoln("Uecontext found. queuing ngap message to uechannel")
		ranUe.AmfUe.EventChannel.UpdateNgapHandler(NgapMsgHandler)
		ngapMsg := context.NgapMsg{
			Ran:       ran,
			NgapMsg:   pdu,
			SctplbMsg: nil,
		}
		if ranUe.Ran.GnbId == ran.GnbId {
			ranUe.AmfUe.TxLog.Infoln("gnbid match")
			ranUe.Ran.Conn = conn
		} else {
			ranUe.AmfUe.TxLog.Infoln("gnbid differ")
			ranUe.AmfUe.TxLog.Infof("In case of Xn handover source RAN gNB id:%s, target RAN gNB id:%s", ranUe.Ran.GnbId, ran.GnbId)
		}
		ranUe.AmfUe.EventChannel.SubmitMessage(ngapMsg)
	} else {
		go DispatchNgapMsg(ctx, ran, pdu, nil)
	}
}

func NgapMsgHandler(ue *context.AmfUe, msg context.NgapMsg) {
	DispatchNgapMsg(ctxt.Background(), msg.Ran, msg.NgapMsg, msg.SctplbMsg)
}

func DispatchNgapMsg(ctx ctxt.Context, ran *context.AmfRan, pdu *ngapType.NGAPPDU, sctplbMsg *sdcoreAmfServer.SctplbMessage) {
	var code int64
	switch pdu.Present {
	case ngapType.NGAPPDUPresentInitiatingMessage:
		if pdu.InitiatingMessage != nil {
			code = pdu.InitiatingMessage.ProcedureCode.Value
		}
	case ngapType.NGAPPDUPresentSuccessfulOutcome:
		if pdu.SuccessfulOutcome != nil {
			code = pdu.SuccessfulOutcome.ProcedureCode.Value
		}
	case ngapType.NGAPPDUPresentUnsuccessfulOutcome:
		if pdu.UnsuccessfulOutcome != nil {
			code = pdu.UnsuccessfulOutcome.ProcedureCode.Value
		}
	}

	peer := "unknown"
	if ran != nil && ran.Conn != nil {
		if addr := ran.Conn.RemoteAddr(); addr != nil {
			peer = addr.String()
		}
	}

	procName := ngapType.ProcedureName(code)
	if procName == "" {
		procName = fmt.Sprintf("UnknownProcedureCode_%d", code)
		logger.AppLog.Warnf("Encountered unknown NGAP procedure code: %d from RAN: %s", code, peer)
	}

	spanName := fmt.Sprintf("AMF NGAP %s", procName)
	ctx, span := tracer.Start(ctx, spanName,
		trace.WithAttributes(
			attribute.String("net.peer", peer),
			attribute.String("ngap.pdu_present", fmt.Sprintf("%d", pdu.Present)),
			attribute.String("ngap.procedureCode", procName),
		),
		trace.WithSpanKind(trace.SpanKindServer),
	)
	defer span.End()

	// Select handler map and extract procedure code based on PDU type
	var handlerMap map[int64]ngapHandlerFunc
	var procedureCode int64

	switch pdu.Present {
	case ngapType.NGAPPDUPresentInitiatingMessage:
		initiatingMessage := pdu.InitiatingMessage
		if initiatingMessage == nil {
			ran.Log.Errorln("Initiating Message is nil")
			return
		}
		handlerMap = initiatingMessageHandlers
		procedureCode = initiatingMessage.ProcedureCode.Value
		metrics.IncrementNgapMsgStats(context.AMF_Self().NfId,
			ngapmsgtypes.NgapMsg[procedureCode],
			"in",
			"",
			"")

	case ngapType.NGAPPDUPresentSuccessfulOutcome:
		successfulOutcome := pdu.SuccessfulOutcome
		if successfulOutcome == nil {
			ran.Log.Errorln("successful Outcome is nil")
			return
		}
		handlerMap = successfulOutcomeHandlers
		procedureCode = successfulOutcome.ProcedureCode.Value
		metrics.IncrementNgapMsgStats(context.AMF_Self().NfId,
			ngapmsgtypes.NgapMsg[procedureCode],
			"in",
			"",
			"")

	case ngapType.NGAPPDUPresentUnsuccessfulOutcome:
		unsuccessfulOutcome := pdu.UnsuccessfulOutcome
		if unsuccessfulOutcome == nil {
			ran.Log.Errorln("unsuccessful Outcome is nil")
			return
		}
		handlerMap = unsuccessfulOutcomeHandlers
		procedureCode = unsuccessfulOutcome.ProcedureCode.Value
		metrics.IncrementNgapMsgStats(context.AMF_Self().NfId,
			ngapmsgtypes.NgapMsg[procedureCode],
			"in",
			"",
			"")

	default:
		ran.Log.Warnf("Unknown PDU present type: %d", pdu.Present)
		return
	}

	// Lookup and call handler from map
	handler := handlerMap[procedureCode]
	if handler == nil {
		ran.Log.Warnf("Not implemented(choice: %d, procedureCode: %d)", pdu.Present, procedureCode)
		return
	}
	handler(ctx, ran, pdu, sctplbMsg)
}

func HandleSCTPNotification(conn net.Conn, notification sctp.Notification) {
	amfSelf := context.AMF_Self()

	logger.NgapLog.Infof("Handle SCTP Notification[addr: %+v]", conn.RemoteAddr())

	ran, ok := amfSelf.AmfRanFindByConn(conn)
	if !ok {
		logger.NgapLog.Warnf("RAN context has been removed[addr: %+v]", conn.RemoteAddr())
		return
	}

	// Removing Stale Connections in AmfRanPool
	amfSelf.AmfRanPool.Range(func(key, value interface{}) bool {
		amfRan := value.(*context.AmfRan)

		conn := amfRan.Conn.(*sctp.SCTPConn)
		errorConn := sctp.NewSCTPConn(-1, nil)
		if reflect.DeepEqual(conn, errorConn) {
			amfRan.Remove()
			ran.Log.Infoln("removed stale entry in AmfRan pool")
		}
		return true
	})

	switch notification.Type() {
	case sctp.SCTP_ASSOC_CHANGE:
		ran.Log.Infoln("SCTP_ASSOC_CHANGE notification")
		event := notification.(*sctp.SCTPAssocChangeEvent)
		switch event.State() {
		case sctp.SCTP_COMM_LOST:
			ran.Log.Infoln("SCTP state is SCTP_COMM_LOST, close the connection")
			ran.Remove()
		case sctp.SCTP_SHUTDOWN_COMP:
			ran.Log.Infoln("SCTP state is SCTP_SHUTDOWN_COMP, close the connection")
			ran.Remove()
		default:
			ran.Log.Warnf("SCTP state[%+v] is not handled", event.State())
		}
	case sctp.SCTP_SHUTDOWN_EVENT:
		ran.Log.Infoln("SCTP_SHUTDOWN_EVENT notification, close the connection")
		ran.Remove()
	default:
		ran.Log.Warnf("Non handled notification type: 0x%x", notification.Type())
	}
}

func HandleSCTPNotificationLb(gnbId string) {
	logger.NgapLog.Infof("Handle SCTP Notification[GnbId: %+v]", gnbId)

	amfSelf := context.AMF_Self()
	ran, ok := amfSelf.AmfRanFindByGnbId(gnbId)
	if !ok {
		logger.NgapLog.Warnf("RAN context has been removed[gnbId: %+v]", gnbId)
		return
	}

	// Removing Stale Connections in AmfRanPool
	amfSelf.AmfRanPool.Range(func(key, value interface{}) bool {
		amfRan := value.(*context.AmfRan)

		if amfRan.GnbId == gnbId {
			amfRan.Remove()
			ran.Log.Infoln("removed stale entry in AmfRan pool")
		}
		return true
	})

	ran.Log.Infoln("SCTP state is SCTP_SHUTDOWN_COMP, close the connection")
	ran.Remove()
}
