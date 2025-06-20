package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/scmhub/ibapi/protobuf"
	"google.golang.org/protobuf/proto"
)

const (
	serverVersion                        = "157"
	apiPrefix                            = "API\x00"
	MIN_SERVER_VER_PROTOBUF              = 152
	MIN_SERVER_VER_OPTIONAL_CAPABILITIES = 100

	// Incoming Client Message IDs
	START_API                       = 71
	REQ_CURRENT_TIME                = 49
	REQ_IDS_CLIENT                  = 4
	PLACE_ORDER_CLIENT              = 3
	REQ_OPEN_ORDERS_CLIENT          = 5
	CANCEL_ORDER_CLIENT             = 7
	REQ_ACCOUNT_UPDATES_CLIENT      = 6
	REQ_EXECUTIONS_CLIENT           = 7
	REQ_CONTRACT_DATA_CLIENT        = 9
	REQ_MKT_DATA_CLIENT             = 1
	REQ_TICK_BY_TICK_DATA_CLIENT    = 94
	CANCEL_TICK_BY_TICK_DATA_CLIENT = 95
	REQ_MKT_DEPTH_CLIENT            = 10
	CANCEL_MKT_DEPTH_CLIENT         = 11
	REQ_HISTORICAL_TICKS_CLIENT     = 97


	// Outgoing Server Message IDs
	NEXT_VALID_ID_SERVER        = 1
	CURRENT_TIME_SERVER         = 3
	MANAGED_ACCTS_SERVER        = 15
	ERR_MSG_SERVER              = 4
	OPEN_ORDER_SERVER           = 5
	ORDER_STATUS_SERVER         = 3
	OPEN_ORDER_END_SERVER       = 6
	ACCT_VALUE_SERVER           = 9
	ACCT_UPDATE_TIME_SERVER     = 12
	EXECUTION_DATA_SERVER       = 8
	EXECUTION_DATA_END_SERVER   = 24
	ACCOUNT_DOWNLOAD_END_SERVER = 11
	CONTRACT_DATA_SERVER        = 10
	CONTRACT_DATA_END_SERVER    = 21
	TICK_PRICE_SERVER           = 1
	TICK_SIZE_SERVER            = 2
	TICK_SNAPSHOT_END_SERVER    = 27
	MARKET_DATA_TYPE_SERVER     = 50
	TICK_BY_TICK_SERVER         = 96
	MARKET_DEPTH_SERVER         = 13
	MARKET_DEPTH_L2_SERVER      = 14
	REAL_TIME_BARS_SERVER       = 52 // From previous subtask, ensure this is correct if used
	HISTORICAL_TICKS_LAST_SERVER = 99
)

type PlacedOrderInfo struct {
	Order         *protobuf.Order
	Contract      *protobuf.Contract
	CurrentStatus string
	FilledQty     string
	RemainingQty  string
	AvgFillPrice  float64
	LastFillPrice float64
}

type StoredExecution struct {
	Execution *protobuf.Execution
	Contract  *protobuf.Contract
}

type clientInfo struct {
	conn                     net.Conn
	clientID                 int64
	clientAPIVersion         int
	clientVersionString      string
	supportsProtobuf         bool
	openOrders               map[int64]*PlacedOrderInfo
	nextServerOrderID        int64
	executions               map[string]*StoredExecution
	nextExecutionID          int64
	accountUpdatesSubscribed bool
	activeMarketData         map[int32]*protobuf.Contract
	cachedContracts          map[int32]*protobuf.ContractDetails
	activeTickByTick         map[int32]string
	activeMarketDepth        map[int32]bool
	// activeRealTimeBars map[int32]*protobuf.Contract // From previous subtask, ensure this is correct if used
}

func strPtr(s string) *string       { return &s }
func float64Ptr(f float64) *float64 { p := f; return &p }
func int32Ptr(i int32) *int32       { p := i; return &p }
func int64Ptr(i int64) *int64       { p := i; return &p }
func boolPtr(b bool) *bool          { p := b; return &p }

func readStringFromBuffer(buf *bytes.Buffer) string {
	str, err := buf.ReadString(0)
	if err != nil && err != io.EOF { return "" }
	return strings.TrimRight(str, "\x00")
}

func boolToASCII(b bool) string {
	if b { return "1" }
	return "0"
}

func (ci *clientInfo) sendErrorMessage(reqID int64, errorCode int32, errorMsg string) {
	var payload []byte
	if ci.supportsProtobuf {
		reqID32 := int32(reqID); errMsgProto := &protobuf.ErrorMessage{Id: int32Ptr(reqID32), ErrorCode: int32Ptr(errorCode), ErrorMsg:  strPtr(errorMsg)}
		var err error; payload, err = proto.Marshal(errMsgProto)
		if err != nil { log.Printf("Client %s: Failed to marshal ErrorMessage: %v", ci.conn.RemoteAddr(), err); return }
	} else { payload = []byte(fmt.Sprintf("2\x00%d\x00%d\x00%s\x00", int32(reqID), errorCode, errorMsg)) }
	if err := ci.sendServerMessage(ERR_MSG_SERVER, payload); err != nil { /* Logged */ }
}

func (ci *clientInfo) sendServerMessage(msgTypeOut int, data []byte) error {
	encodedMsg, err := encodeMessage(msgTypeOut, data, ci.supportsProtobuf)
	if err != nil { log.Printf("Client %s: Error encoding message type %d: %v", ci.conn.RemoteAddr(), msgTypeOut, err); return err }
	_, err = ci.conn.Write(encodedMsg)
	if err != nil {log.Printf("Client %s: Error sending message type %d: %v", ci.conn.RemoteAddr(), msgTypeOut, err)}
	return err
}

// --- Existing Handlers (handleStartAPI, etc. - truncated for brevity but present in actual file) ---
func handleStartAPI(rawPayload []byte, ci *clientInfo) { /* ... */
	parts := bytes.SplitN(rawPayload, []byte{0}, 4); if len(parts) < 2 { ci.sendErrorMessage(-1, 500, "Malformed START_API message."); return }
	clientVersionStr := string(parts[0]); clientIDStr := string(parts[1]); optionalCapabilitiesStr := ""; if len(parts) > 2 && len(parts[2]) > 0 { optionalCapabilitiesStr = string(parts[2]) }
	parsedAPIVersion, errV := strconv.Atoi(clientVersionStr); parsedClientID, errC := strconv.ParseInt(clientIDStr, 10, 64)
	if errV != nil || errC != nil { ci.sendErrorMessage(-1, 500, "Invalid version/clientID in START_API."); return }
	ci.clientAPIVersion = parsedAPIVersion; ci.clientID = parsedClientID
	log.Printf("Client %s: Parsed START_API: ClientAPIVersion=%d, ClientID=%d, OptCaps='%s'", ci.conn.RemoteAddr(), ci.clientAPIVersion, ci.clientID, optionalCapabilitiesStr)
	var nextValidIDPayload []byte; if ci.supportsProtobuf { nextValidIDPayload = []byte{0x08, 0x01} } else { nextValidIDPayload = []byte("1\x001\x00") }
	ci.sendServerMessage(NEXT_VALID_ID_SERVER, nextValidIDPayload)
	managedAccountsPayload := []byte("1\x00DU12345\x00"); ci.sendServerMessage(MANAGED_ACCTS_SERVER, managedAccountsPayload)
}
func handleReqCurrentTime(data []byte, ci *clientInfo) { /* ... */ currentTime := time.Now().Unix(); currentTimePayload := []byte(fmt.Sprintf("1\x00%d\x00", currentTime)); ci.sendServerMessage(CURRENT_TIME_SERVER, currentTimePayload) }
func handleReqIDs(data []byte, ci *clientInfo) { /* ... */  var nextValidIDPayload []byte; if ci.supportsProtobuf {  nextValidIDPayload = []byte{0x08, 0x01}  } else {  nextValidIDPayload = []byte("1\x001\x00")  }; ci.sendServerMessage(NEXT_VALID_ID_SERVER, nextValidIDPayload) }
func handleCancelOrderProto(req *protobuf.CancelOrderRequest, ci *clientInfo) { /* ... */
	orderIDToCancel := req.GetOrderId(); log.Printf("Client %s: Protobuf CANCEL_ORDER for OrderID: %d", ci.conn.RemoteAddr(), orderIDToCancel)
	if pOI, found := ci.openOrders[int64(orderIDToCancel)]; found {
		pOI.CurrentStatus = "Cancelled"; pOI.RemainingQty = "0"; pOI.FilledQty = pOI.FilledQty
		status := &protobuf.OrderStatus{OrderId: int32Ptr(orderIDToCancel), Status: strPtr(pOI.CurrentStatus), Filled: strPtr(pOI.FilledQty), Remaining: strPtr(pOI.RemainingQty), AvgFillPrice: float64Ptr(pOI.AvgFillPrice), LastFillPrice: float64Ptr(pOI.LastFillPrice), PermId: pOI.Order.PermId, ParentId: int32Ptr(pOI.Order.GetParentId()), ClientId: int32Ptr(int32(ci.clientID))}
		payload, _ := proto.Marshal(status); ci.sendServerMessage(ORDER_STATUS_SERVER, payload)
	} else { ci.sendErrorMessage(int64(orderIDToCancel), 202, "Order Canceled - Reason: Order not found") }
}
func handleCancelOrder(data []byte, ci *clientInfo) { /* ... */
	parts := bytes.SplitN(data, []byte{0}, 2); if len(parts) < 1 { ci.sendErrorMessage(-1, 500, "Malformed Legacy CancelOrder."); return }
	oIDStr := string(parts[0]); pID, err := strconv.Atoi(oIDStr); if err != nil { ci.sendErrorMessage(-1, 500, "Invalid legacy OrderID for cancel."); return }
	oID := int32(pID)
	if pOI, found := ci.openOrders[int64(oID)]; found {
		pOI.CurrentStatus = "Cancelled"; pOI.RemainingQty = "0"
		legacyStatus := fmt.Sprintf("3\x00%d\x00%s\x00%s\x00%s\x00%f\x00%d\x00%d\x00%f\x00%d\x00%s\x00", oID, pOI.CurrentStatus, pOI.FilledQty, pOI.RemainingQty, pOI.AvgFillPrice, pOI.Order.GetPermId(), pOI.Order.GetParentId(), pOI.LastFillPrice, int32(ci.clientID), "")
		ci.sendServerMessage(ORDER_STATUS_SERVER, []byte(legacyStatus));
	} else { ci.sendErrorMessage(int64(oID), 202, "Order Canceled - Reason: Order not found") }
}
func handleReqExecutionsProto(req *protobuf.ExecutionRequest, ci *clientInfo) { /* ... */
	reqID := req.GetReqId(); log.Printf("Client %s: Protobuf REQ_EXECUTIONS for ReqId: %d", ci.conn.RemoteAddr(), reqID)
	for execID, sExec := range ci.executions { log.Printf("Client %s: Sending exec %s for ReqId: %d", ci.conn.RemoteAddr(), execID, reqID); details := &protobuf.ExecutionDetails{ReqId: int32Ptr(reqID), Contract: sExec.Contract, Execution: sExec.Execution}; payload, _ := proto.Marshal(details); ci.sendServerMessage(EXECUTION_DATA_SERVER, payload) }
	endProto := &protobuf.ExecutionDetailsEnd{ReqId: int32Ptr(reqID)}; payload, _ := proto.Marshal(endProto); ci.sendServerMessage(EXECUTION_DATA_END_SERVER, payload)
}
func handleReqExecutions(data []byte, ci *clientInfo) { /* ... */
	parts := bytes.SplitN(data, []byte{0}, 2); reqID := int32(-1); if len(parts) > 0 { pID, err := strconv.Atoi(string(parts[0])); if err == nil { reqID = int32(pID) } }
	log.Printf("Client %s: Legacy REQ_EXECUTIONS for ReqId: %d. Not sending all executions.", ci.conn.RemoteAddr(), reqID)
	ci.sendServerMessage(EXECUTION_DATA_END_SERVER, []byte(fmt.Sprintf("1\x00%d\x00", reqID)))
}
func handlePlaceOrder(data []byte, ci *clientInfo) { /* ... */
	req := &protobuf.PlaceOrderRequest{}; var orderID int32 = 0; var order *protobuf.Order; var contract *protobuf.Contract
	if ci.supportsProtobuf { if err := proto.Unmarshal(data, req); err != nil { ci.sendErrorMessage(-1, 500, "Failed to parse PlaceOrderRequest."); return }; order = req.GetOrder(); contract = req.GetContract(); if order != nil { orderID = order.GetOrderId() }
	} else {
		fields := bytes.Split(data, []byte{0}); if len(fields) < 15 { ci.sendErrorMessage(-1, 500, "Malformed Legacy PlaceOrder."); return }
		parsedID, err := strconv.Atoi(string(fields[1])); if err != nil { ci.sendErrorMessage(-1, 500, "Invalid legacy OrderID."); return }
		orderID = int32(parsedID); lmtPriceVal, _ := strconv.ParseFloat(string(fields[14]), 64)
		order = &protobuf.Order{ OrderId: &orderID, Action: strPtr(string(fields[11])), TotalQuantity: strPtr(string(fields[12])), OrderType: strPtr(string(fields[13])), LmtPrice: float64Ptr(lmtPriceVal), Account: strPtr(string(fields[10]))}
		contract = &protobuf.Contract{Symbol: strPtr(string(fields[3])), SecType: strPtr(string(fields[4])), Exchange: strPtr(string(fields[8]))}
	}
	if order == nil || contract == nil { ci.sendErrorMessage(int64(orderID), 500, "PlaceOrder missing Order/Contract."); return }
	cOID := order.GetOrderId(); if cOID == 0 { ci.sendErrorMessage(0, 500, "Invalid OrderId (0)."); return }
	iRQ := "0"; if order.TotalQuantity != nil { iRQ = *order.TotalQuantity }; pOI := &PlacedOrderInfo{Order:order,Contract:contract,CurrentStatus:"Submitted",FilledQty:"0",RemainingQty:iRQ,AvgFillPrice:0.0,LastFillPrice:0.0}; ci.openOrders[int64(cOID)] = pOI
	log.Printf("Client %s: Placed OrderID %d: %s %s @ %.2f", ci.conn.RemoteAddr(),cOID,order.GetAction(),order.GetTotalQuantity(),order.GetLmtPrice())
	sS := &protobuf.OrderStatus{OrderId:int32Ptr(cOID),Status:strPtr(pOI.CurrentStatus),Filled:strPtr(pOI.FilledQty),Remaining:strPtr(pOI.RemainingQty),AvgFillPrice:float64Ptr(pOI.AvgFillPrice),PermId:order.PermId,ParentId:int32Ptr(order.GetParentId()),LastFillPrice:float64Ptr(pOI.LastFillPrice),ClientId:int32Ptr(int32(ci.clientID)),WhyHeld:strPtr("")}
	pS,_:=proto.Marshal(sS);ci.sendServerMessage(ORDER_STATUS_SERVER,pS)
	oOS := &protobuf.OrderState{Status:strPtr(pOI.CurrentStatus)};oOM:=&protobuf.OpenOrder{Order:order,Contract:contract,OrderState:oOS};pOO,_:=proto.Marshal(oOM);ci.sendServerMessage(OPEN_ORDER_SERVER,pOO)
	eIDStr:=fmt.Sprintf("exec-%d-%d",cOID,ci.nextExecutionID);ci.nextExecutionID++; pOI.CurrentStatus="Filled";if order.TotalQuantity!=nil{pOI.FilledQty=*order.TotalQuantity};pOI.RemainingQty="0";pOI.AvgFillPrice=order.GetLmtPrice();pOI.LastFillPrice=order.GetLmtPrice()
	exec:=&protobuf.Execution{ExecId:strPtr(eIDStr),OrderId:int32Ptr(cOID),Time:strPtr(time.Now().Format("20060102-15:04:05")),AcctNumber:order.Account,Exchange:contract.Exchange,Side:order.Action,Shares:strPtr(pOI.FilledQty),Price:float64Ptr(pOI.LastFillPrice),PermId:order.PermId,ClientId:int32Ptr(int32(ci.clientID)),CumQty:strPtr(pOI.FilledQty),AvgPrice:float64Ptr(pOI.AvgFillPrice)}
	ci.executions[eIDStr]=&StoredExecution{Execution:exec,Contract:contract};eD:=&protobuf.ExecutionDetails{ReqId:int32Ptr(cOID),Contract:contract,Execution:exec};pE,_:=proto.Marshal(eD);ci.sendServerMessage(EXECUTION_DATA_SERVER,pE)
	sF:=&protobuf.OrderStatus{OrderId:int32Ptr(cOID),Status:strPtr(pOI.CurrentStatus),Filled:strPtr(pOI.FilledQty),Remaining:strPtr(pOI.RemainingQty),AvgFillPrice:float64Ptr(pOI.AvgFillPrice),PermId:order.PermId,ParentId:int32Ptr(order.GetParentId()),LastFillPrice:float64Ptr(pOI.LastFillPrice),ClientId:int32Ptr(int32(ci.clientID)),WhyHeld:strPtr("")}
	pF,_:=proto.Marshal(sF);ci.sendServerMessage(ORDER_STATUS_SERVER,pF)
}
func handleReqAccountUpdates(data []byte, ci *clientInfo) { /* ... */
	actualDataParts := bytes.SplitN(data, []byte{0}, 2); if len(actualDataParts) < 1 { ci.sendErrorMessage(-1, 500, "Malformed REQ_ACCOUNT_UPDATES."); return }
	subscribeStr := string(actualDataParts[0]); accountCodeStr := "DU12345"; if len(actualDataParts) > 1 && len(actualDataParts[1]) > 0 { accountCodeStr = string(actualDataParts[1]) }
	subscribe := subscribeStr == "true" || subscribeStr == "1"; wasSubscribed := ci.accountUpdatesSubscribed; ci.accountUpdatesSubscribed = subscribe
	log.Printf("Client %s: Acct updates subscription: %t for %s (was %t)", ci.conn.RemoteAddr(), subscribe, accountCodeStr, wasSubscribed)
	if subscribe {
		acctVals := [][3]string{{"AccountCode", accountCodeStr, "USD"}, {"NetLiquidation", "250K", "USD"}, {"TotalCashValue", "250K", "USD"}}; for _, v := range acctVals { ci.sendServerMessage(ACCT_VALUE_SERVER, []byte(fmt.Sprintf("1\x00%s\x00%s\x00%s\x00%s\x00", v[0], v[1], v[2], accountCodeStr))) }
		ci.sendServerMessage(ACCT_UPDATE_TIME_SERVER, []byte(fmt.Sprintf("1\x00%s\x00", time.Now().Format("15:04:05"))))
	} else if wasSubscribed { ci.sendServerMessage(ACCOUNT_DOWNLOAD_END_SERVER, []byte(fmt.Sprintf("1\x00%s\x00", accountCodeStr))) }
}
func handleReqOpenOrders(data []byte, ci *clientInfo) { /* ... */
	log.Printf("Client %s: Handling REQ_OPEN_ORDERS", ci.conn.RemoteAddr())
	if !ci.supportsProtobuf { log.Printf("Client %s: Legacy REQ_OPEN_ORDERS.", ci.conn.RemoteAddr()) }
	for orderID, placedOrderInfo := range ci.openOrders {
		order := placedOrderInfo.Order; contract := placedOrderInfo.Contract
		log.Printf("Client %s: Sending open order ID %d, Status: %s", ci.conn.RemoteAddr(), orderID, placedOrderInfo.CurrentStatus)
		orderState := &protobuf.OrderState{Status: strPtr(placedOrderInfo.CurrentStatus)}
		openOrderMsg := &protobuf.OpenOrder{Order: order, Contract: contract, OrderState: orderState}
		payloadOpen, _ := proto.Marshal(openOrderMsg); ci.sendServerMessage(OPEN_ORDER_SERVER, payloadOpen)
		oID := order.GetOrderId()
		status := &protobuf.OrderStatus{ OrderId: int32Ptr(oID), Status: strPtr(placedOrderInfo.CurrentStatus), Filled: strPtr(placedOrderInfo.FilledQty), Remaining: strPtr(placedOrderInfo.RemainingQty), AvgFillPrice: float64Ptr(placedOrderInfo.AvgFillPrice), PermId: order.PermId, ParentId: int32Ptr(order.GetParentId()), LastFillPrice: float64Ptr(placedOrderInfo.LastFillPrice), ClientId: int32Ptr(int32(ci.clientID)), WhyHeld: strPtr("")}
		payloadStatus, _ := proto.Marshal(status); ci.sendServerMessage(ORDER_STATUS_SERVER, payloadStatus)
	}
	var endPayload []byte; if ci.supportsProtobuf { endProto := &protobuf.OpenOrdersEnd{}; payload, err := proto.Marshal(endProto); if err != nil {return}; endPayload = payload } else { endPayload = []byte("1\x00") }
	ci.sendServerMessage(OPEN_ORDER_END_SERVER, endPayload)
}
func handleReqContractData(data []byte, ci *clientInfo) { /* ... */
	log.Printf("Client %s: Handling REQ_CONTRACT_DATA", ci.conn.RemoteAddr())
	reqID := int32(0); var reqContract *protobuf.Contract
	if ci.supportsProtobuf {
		req := &protobuf.ContractDataRequest{}; if err := proto.Unmarshal(data, req); err != nil { ci.sendErrorMessage(-1, 500, "Failed to parse ContractDataRequest."); if req!=nil{reqID=req.GetReqId()}; endPayload, _ := proto.Marshal(&protobuf.ContractDataEnd{ReqId: int32Ptr(reqID)}); ci.sendServerMessage(CONTRACT_DATA_END_SERVER, endPayload); return }
		reqID = req.GetReqId(); reqContract = req.GetContract()
		log.Printf("Client %s: Protobuf REQ_CONTRACT_DATA for ReqId: %d, Contract criteria: %v", ci.conn.RemoteAddr(), reqID, reqContract)
		var foundDetails *protobuf.ContractDetails
		if reqContract != nil && reqContract.GetConId() != 0 { foundDetails = ci.cachedContracts[reqContract.GetConId()]
		} else if reqContract != nil && reqContract.GetSymbol() != "" {
			for _, details := range ci.cachedContracts { if details.GetMarketName() == reqContract.GetSymbol() { foundDetails = details; break } }
		}
		if foundDetails != nil {
			contractData := &protobuf.ContractData{ReqId: int32Ptr(reqID), Contract: reqContract, ContractDetails: foundDetails}
			payload, err := proto.Marshal(contractData); if err == nil { ci.sendServerMessage(CONTRACT_DATA_SERVER, payload) } else { log.Printf("Client %s: Failed to marshal ContractData: %v", ci.conn.RemoteAddr(), err)}
		} else { log.Printf("Client %s: No matching contract for ReqId: %d", ci.conn.RemoteAddr(), reqID) }
		endPayload, _ := proto.Marshal(&protobuf.ContractDataEnd{ReqId: int32Ptr(reqID)}); ci.sendServerMessage(CONTRACT_DATA_END_SERVER, endPayload)
	} else {
		log.Printf("Client %s: Legacy REQ_CONTRACT_DATA not fully implemented.", ci.conn.RemoteAddr())
		parts := bytes.Split(data, []byte{0}); if len(parts) > 1 { legacyReqID, err := strconv.Atoi(string(parts[1])); if err == nil {	reqID = int32(legacyReqID) } }
		ci.sendServerMessage(CONTRACT_DATA_END_SERVER, []byte(fmt.Sprintf("1\x00%d\x00", reqID)))
	}
}
func handleReqMktData(data []byte, ci *clientInfo) { /* ... */
	log.Printf("Client %s: Handling REQ_MKT_DATA", ci.conn.RemoteAddr())
	buf := bytes.NewBuffer(data); var version int32; var reqID int64; var conID int32; var snapshot bool; var regulatorySnapshot bool; var genericTickList string
	if err := binary.Read(buf, binary.BigEndian, &version); err != nil { ci.sendErrorMessage(-1, 500, "Parse error (mktData version)"); return }
	if err := binary.Read(buf, binary.BigEndian, &reqID); err != nil { ci.sendErrorMessage(reqID, 500, "Parse error (mktData reqId)"); return }
	requestedContract := &protobuf.Contract{}; if err := binary.Read(buf, binary.BigEndian, &conID); err == nil { requestedContract.ConId = int32Ptr(conID) }
	requestedContract.Symbol = strPtr(readStringFromBuffer(buf)); requestedContract.SecType = strPtr(readStringFromBuffer(buf))
	requestedContract.LastTradeDateOrContractMonth = strPtr(readStringFromBuffer(buf)); var strike float64
	if err := binary.Read(buf, binary.BigEndian, &strike); err == nil { requestedContract.Strike = float64Ptr(strike) } else { requestedContract.Strike = float64Ptr(0.0)}
	requestedContract.Right = strPtr(readStringFromBuffer(buf));
	multiplierStr := readStringFromBuffer(buf); if multiplierStr != "" { multVal, err := strconv.ParseFloat(multiplierStr, 64); if err == nil { requestedContract.Multiplier = float64Ptr(multVal) } }
	requestedContract.Exchange = strPtr(readStringFromBuffer(buf)); requestedContract.PrimaryExch = strPtr(readStringFromBuffer(buf))
	requestedContract.Currency = strPtr(readStringFromBuffer(buf)); requestedContract.LocalSymbol = strPtr(readStringFromBuffer(buf)); requestedContract.TradingClass = strPtr(readStringFromBuffer(buf))
	genericTickList = readStringFromBuffer(buf)
	var snapshotByte byte; if err := binary.Read(buf, binary.BigEndian, &snapshotByte); err == nil { snapshot = (snapshotByte != 0) } else {ci.sendErrorMessage(reqID, 500, "Parse error (mktData snapshot flag)"); return}
	if version >= 9 { var regSnapshotByte byte; if err := binary.Read(buf, binary.BigEndian, &regSnapshotByte); err == nil { regulatorySnapshot = (regSnapshotByte != 0) } }
	log.Printf("Client %s: REQ_MKT_DATA ReqId: %d, Contract: ConId=%d,Sym=%s,Sec=%s,Exch=%s,Snap=%t,Ticks=%s,RegSnap=%t", ci.conn.RemoteAddr(), reqID, requestedContract.GetConId(), requestedContract.GetSymbol(), requestedContract.GetSecType(), requestedContract.GetExchange(), snapshot, genericTickList, regulatorySnapshot)
	ci.activeMarketData[int32(reqID)] = requestedContract
	marketDataTypePayload := fmt.Sprintf("1\x00%d\x001\x00", reqID); ci.sendServerMessage(MARKET_DATA_TYPE_SERVER, []byte(marketDataTypePayload))
	if snapshot {
		reqID32 := int32(reqID)
		log.Printf("Client %s: Sending snapshot end for ReqId: %d (Specific tick messages omitted)", ci.conn.RemoteAddr(), reqID32)
		ci.sendServerMessage(TICK_SNAPSHOT_END_SERVER, []byte(fmt.Sprintf("1\x00%d\x00", reqID32)))
	} else { log.Printf("Client %s: Streaming market data not implemented for ReqId: %d.", ci.conn.RemoteAddr(), reqID) }
}

func handleReqTickByTickData(data []byte, ci *clientInfo) {
	log.Printf("Client %s: Handling REQ_TICK_BY_TICK_DATA, data len: %d", ci.conn.RemoteAddr(), len(data))
	buf := bytes.NewBuffer(data); var reqID int64; var conID int32; var numberOfTicks int64; var ignoreSize bool; var tickTypeStr string
	// Payload structure: ReqId (int64), ConId (int32), Symbol (string), SecType (string), LastTradeDateOrContractMonth (string), Strike (float64), Right (string), Multiplier (string), Exchange (string), PrimaryExchange (string), Currency (string), LocalSymbol (string), TradingClass (string), TickType (string), NumberOfTicks (int64), IgnoreSize (bool)
	if err := binary.Read(buf, binary.BigEndian, &reqID); err != nil { ci.sendErrorMessage(-1, 500, "Parse error (tickData reqId)"); return }
	requestedContract := &protobuf.Contract{}
	if err := binary.Read(buf, binary.BigEndian, &conID); err == nil { requestedContract.ConId = int32Ptr(conID) } // conId is int32 in Contract proto
	requestedContract.Symbol = strPtr(readStringFromBuffer(buf)); requestedContract.SecType = strPtr(readStringFromBuffer(buf))
	requestedContract.LastTradeDateOrContractMonth = strPtr(readStringFromBuffer(buf)); var strike float64
	if err := binary.Read(buf, binary.BigEndian, &strike); err == nil { requestedContract.Strike = float64Ptr(strike) } else { requestedContract.Strike = float64Ptr(0.0)}
	requestedContract.Right = strPtr(readStringFromBuffer(buf));
	multiplierStr := readStringFromBuffer(buf); if multiplierStr != "" { multVal, err := strconv.ParseFloat(multiplierStr, 64); if err == nil { requestedContract.Multiplier = float64Ptr(multVal) } }
	requestedContract.Exchange = strPtr(readStringFromBuffer(buf)); requestedContract.PrimaryExch = strPtr(readStringFromBuffer(buf))
	requestedContract.Currency = strPtr(readStringFromBuffer(buf)); requestedContract.LocalSymbol = strPtr(readStringFromBuffer(buf)); requestedContract.TradingClass = strPtr(readStringFromBuffer(buf))
	tickTypeStr = readStringFromBuffer(buf)
	if err := binary.Read(buf, binary.BigEndian, &numberOfTicks); err != nil { ci.sendErrorMessage(reqID, 500, "Parse error (tickData numberOfTicks)"); return }
	var ignoreSizeByte byte; if err := binary.Read(buf, binary.BigEndian, &ignoreSizeByte); err == nil { ignoreSize = (ignoreSizeByte != 0) } else { ci.sendErrorMessage(reqID, 500, "Parse error (tickData ignoreSize)"); return }
	log.Printf("Client %s: REQ_TICK_BY_TICK_DATA ReqId: %d, Contract: %v, TickType: %s, NumTicks: %d, IgnoreSize: %t", ci.conn.RemoteAddr(), reqID, requestedContract, tickTypeStr, numberOfTicks, ignoreSize)
	ci.activeTickByTick[int32(reqID)] = tickTypeStr
	reqIDStr := strconv.FormatInt(reqID, 10); nul := "\x00"; timestamp := time.Now().Unix()
	for i := 0; i < 2; i++ {
		currentTs := timestamp + int64(i); var tickPayload string
		if tickTypeStr == "Last" || tickTypeStr == "AllLast" {
			tickTypeVal := "1"; if tickTypeStr == "AllLast" { tickTypeVal = "2"}
			tickPayload = strings.Join([]string{reqIDStr, tickTypeVal, fmt.Sprintf("%d", currentTs), fmt.Sprintf("150.2%d", i), fmt.Sprintf("1%d",i), "0", "SMART", ""}, nul) + nul
		} else if tickTypeStr == "BidAsk" {
			tickPayload = strings.Join([]string{reqIDStr, "3", fmt.Sprintf("%d", currentTs), fmt.Sprintf("150.0%d",i), fmt.Sprintf("150.5%d",i), fmt.Sprintf("%d",5+i), fmt.Sprintf("%d",8+i), "0"}, nul) + nul
		} else if tickTypeStr == "MidPoint" {
			tickPayload = strings.Join([]string{reqIDStr, "4", fmt.Sprintf("%d", currentTs), fmt.Sprintf("150.1%d",i)}, nul) + nul
		}
		if tickPayload != "" { ci.sendServerMessage(TICK_BY_TICK_SERVER, []byte(tickPayload)) }
	}
	if numberOfTicks > 0 { delete(ci.activeTickByTick, int32(reqID)) }
}
func handleCancelTickByTickData(data []byte, ci *clientInfo) { /* ... */
	// Payload for CANCEL_TICK_BY_TICK_DATA is just ReqId (int64), after version.
	// decodeMessage gives data *after* the main message ID.
	// So, data here starts with Version (int32), then ReqId (int64).
	buf := bytes.NewBuffer(data); var version int32; var reqID int64
	if err := binary.Read(buf, binary.BigEndian, &version); err != nil { ci.sendErrorMessage(-1, 500, "Parse error (cancelTickByTickData version)"); return }
	if err := binary.Read(buf, binary.BigEndian, &reqID); err != nil { ci.sendErrorMessage(-1, 500, "Parse error (cancelTickByTickData reqId)"); return }
	log.Printf("Client %s: Request to cancel tick-by-tick data for ReqId: %d, Version: %d", ci.conn.RemoteAddr(), reqID, version)
	delete(ci.activeTickByTick, int32(reqID))
}
func mustMarshal(pb proto.Message) []byte { payload, err := proto.Marshal(pb); if err != nil { log.Printf("Error marshaling %T: %v", pb, err) }; return payload }

func decodeMessage(reader io.Reader, clientSupportsProtobuf bool) (msgType int, msgData []byte, err error) { /* ... */
	var size uint32; if err = binary.Read(reader, binary.BigEndian, &size); err != nil { return 0, nil, fmt.Errorf("read size err: %w", err) }
	if size == 0 { return 0, nil, fmt.Errorf("msg size 0") }
	payload := make([]byte, size); if _, err = io.ReadFull(reader, payload); err != nil { return 0, nil, fmt.Errorf("read payload err: %w", err) }
	if clientSupportsProtobuf {
		if len(payload) < 4 { return 0, nil, fmt.Errorf("proto envelope too short for ID: %d", len(payload)) }
		msgType = int(binary.BigEndian.Uint32(payload[:4])); msgData = payload[4:]
	} else {
		parts := bytes.SplitN(payload, []byte{0}, 2)
		if len(parts[0]) == 0 { return 0, nil, fmt.Errorf("legacy msg ID empty") }
		msgType, err = strconv.Atoi(string(parts[0])); if err != nil { return 0, nil, fmt.Errorf("legacy msg ID atoi err: %w", err) }
		if len(parts) > 1 { msgData = parts[1] } else { msgData = []byte{} }
	}
	return msgType, msgData, nil
}
func encodeMessage(msgTypeOut int, msgData []byte, clientCanReceiveProto bool) ([]byte, error) { /* ... */
	var bufWithID []byte
	if clientCanReceiveProto { idBytes := make([]byte, 4); binary.BigEndian.PutUint32(idBytes, uint32(msgTypeOut)); bufWithID = append(idBytes, msgData...)
	} else { idStr := strconv.Itoa(msgTypeOut) + "\x00"; bufWithID = append([]byte(idStr), msgData...) }
	size := uint32(len(bufWithID)); finalBuf := new(bytes.Buffer)
	if err := binary.Write(finalBuf, binary.BigEndian, size); err != nil { return nil, fmt.Errorf("encode size err: %w", err) }
	if _, err := finalBuf.Write(bufWithID); err != nil { return nil, fmt.Errorf("encode body err: %w", err) }
	return finalBuf.Bytes(), nil
}

func handleConnection(conn net.Conn) { /* ... */
	defer conn.Close(); log.Printf("Client connected: %s", conn.RemoteAddr().String())
	prefixBuf := make([]byte, len(apiPrefix)); if _, err := io.ReadFull(conn, prefixBuf); err != nil { log.Printf("API prefix read err: %v", err); return }
	if string(prefixBuf) != apiPrefix { log.Printf("Invalid API prefix"); return }
	var clientVerLen uint32; if err := binary.Read(conn, binary.BigEndian, &clientVerLen); err != nil { log.Printf("Client ver len read err: %v", err); return }
	clientVerBuf := make([]byte, clientVerLen); if _, err := io.ReadFull(conn, clientVerBuf); err != nil { log.Printf("Client ver read err: %v", err); return }
	clientVersion := string(clientVerBuf); serverTime := time.Now().Format("20060102 15:04:05")
	serverResp := fmt.Sprintf("%s\x00%s\x00", serverVersion, serverTime); if _, err := conn.Write([]byte(serverResp)); err != nil { log.Printf("Server ver send err: %v", err); return }
	log.Printf("Sent server version to %s", conn.RemoteAddr().String()); serverVerInt, _ := strconv.Atoi(serverVersion)
	client := &clientInfo{
		conn: conn, supportsProtobuf: serverVerInt >= MIN_SERVER_VER_PROTOBUF, clientVersionString: clientVersion,
		openOrders: make(map[int64]*PlacedOrderInfo), executions: make(map[string]*StoredExecution),
		activeMarketData: make(map[int32]*protobuf.Contract), cachedContracts: make(map[int32]*protobuf.ContractDetails),
		activeTickByTick: make(map[int32]string),
		nextServerOrderID: 1, nextExecutionID: 1, accountUpdatesSubscribed: false,
	}
	client.cachedContracts[101] = &protobuf.ContractDetails{ MarketName: strPtr("APPLE INC"), MinTick: strPtr("0.01"), OrderTypes: strPtr("LMT,MKT"), ValidExchanges: strPtr("SMART,NASDAQ,NYSE"), LongName: strPtr("APPLE INC"), ContractMonth: strPtr("")}
	client.cachedContracts[202] = &protobuf.ContractDetails{ MarketName: strPtr("E-MINI S&P 500"), MinTick: strPtr("0.25"), OrderTypes: strPtr("LMT,MKT"), ValidExchanges: strPtr("CME,GLOBEX"), LongName: strPtr("E-MINI S&P 500 FUTURES"), ContractMonth: strPtr("202403")}
	log.Printf("Client %s (%s) configured. Proto: %t", client.conn.RemoteAddr(), client.clientVersionString, client.supportsProtobuf)
	for {
		msgType, msgData, err := decodeMessage(client.conn, client.supportsProtobuf); if err != nil { log.Printf("Client %s disconnected: %v", client.conn.RemoteAddr(), err); return }
		if msgType == START_API { handleStartAPI(msgData, client)
		} else if client.supportsProtobuf {
			switch msgType {
			case REQ_CURRENT_TIME: handleReqCurrentTime(msgData, client)
			case REQ_IDS_CLIENT: handleReqIDs(msgData, client)
			case PLACE_ORDER_CLIENT: handlePlaceOrder(msgData, client)
			case REQ_OPEN_ORDERS_CLIENT: handleReqOpenOrders(msgData, client)
			case REQ_ACCOUNT_UPDATES_CLIENT: handleReqAccountUpdates(msgData, client)
			case REQ_CONTRACT_DATA_CLIENT: handleReqContractData(msgData, client)
			case REQ_MKT_DATA_CLIENT: handleReqMktData(msgData, client)
			case REQ_TICK_BY_TICK_DATA_CLIENT: handleReqTickByTickData(msgData, client)
			case CANCEL_TICK_BY_TICK_DATA_CLIENT: handleCancelTickByTickData(msgData, client)
			case CANCEL_ORDER_CLIENT: // This is 7. Also REQ_EXECUTIONS_CLIENT if its proto ID is 7.
				var cReq protobuf.CancelOrderRequest; if err := proto.Unmarshal(msgData, &cReq); err == nil && cReq.OrderId != nil && *cReq.OrderId != 0 { handleCancelOrderProto(&cReq, client)
				} else { var eReq protobuf.ExecutionRequest; if err := proto.Unmarshal(msgData, &eReq); err == nil { handleReqExecutionsProto(&eReq, client)
					} else { client.sendErrorMessage(0, 500, "Invalid Protobuf message for shared type ID 7.") }
				}
			default: client.sendErrorMessage(-1, 501, fmt.Sprintf("Unhandled proto msg type: %d", msgType))
			}
		} else { // Legacy
			switch msgType {
			case REQ_CURRENT_TIME: handleReqCurrentTime(msgData, client)
			case REQ_IDS_CLIENT: handleReqIDs(msgData, client)
			case PLACE_ORDER_CLIENT: handlePlaceOrder(msgData, client)
			case REQ_OPEN_ORDERS_CLIENT: handleReqOpenOrders(msgData, client)
			case REQ_ACCOUNT_UPDATES_CLIENT: handleReqAccountUpdates(msgData, client)
			case REQ_CONTRACT_DATA_CLIENT: handleReqContractData(msgData, client)
			case REQ_MKT_DATA_CLIENT: handleReqMktData(msgData, client)
			case REQ_TICK_BY_TICK_DATA_CLIENT: handleReqTickByTickData(msgData, client)
			case CANCEL_TICK_BY_TICK_DATA_CLIENT: handleCancelTickByTickData(msgData, client)
			case CANCEL_ORDER_CLIENT: // Legacy ID 7 (covers both Cancel Order V1 and Req Executions V3)
				fields := bytes.SplitN(msgData, []byte{0}, 2); if len(fields) > 0 {
					verStr := string(fields[0]); var content []byte; if len(fields) > 1 { content = fields[1] }
					if verStr == "1" { handleCancelOrder(content, client)
					} else if verStr == "3" { handleReqExecutions(content, client)
					} else { client.sendErrorMessage(0, 504, "Unsupported legacy version for ID 7.") }
				} else { client.sendErrorMessage(0, 500, "Malformed legacy message for ID 7.") }
			default: client.sendErrorMessage(-1, 501, fmt.Sprintf("Unhandled legacy msg type: %d", msgType))
			}
		}
	}
}

func main() { /* ... */
	port := flag.Int("port", 4001, "Port to listen on")
	flag.Parse()
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil { log.Fatalf("Failed to start server: %v", err); os.Exit(1) }
	defer listener.Close()
	log.Printf("Server started, listening on port %d", *port)
	for {
		conn, errListener := listener.Accept()
		if errListener != nil { log.Printf("Failed to accept connection: %v", errListener); continue }
		go handleConnection(conn)
	}
}
