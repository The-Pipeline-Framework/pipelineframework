
Ë
#input_csv_file_processing_svc.proto"√
PaymentRecord
id (	Rid
csvId (	RcsvId
	recipient (	R	recipient
amount (	Ramount
currency (	Rcurrency:
csvPaymentsInputFilePath (	RcsvPaymentsInputFilePath"h
CsvPaymentsInputFile
id (	Rid
filepath (	Rfilepath$
csvFolderPath (	RcsvFolderPath"0
CsvPaymentsInputStream
source (	Rsource"
	CsvFolder
path (	Rpath2b
&ProcessCsvPaymentsInputReactiveService8
remoteProcess.CsvPaymentsInputFile.PaymentRecord02b
$ProcessCsvPaymentsInputStreamService:
remoteProcess.CsvPaymentsInputStream.PaymentRecord02L
ProcessFolderService4
remoteProcess
.CsvFolder.CsvPaymentsInputFile0B 
org.pipelineframework.csv.grpcbproto3
õ	
payments_processing_svc.proto#input_csv_file_processing_svc.proto"
SendPaymentRequest
msisdn (	Rmsisdn
amount (	Ramount
currency (	Rcurrency
	reference (	R	reference
url (	Rurl(
paymentRecordId (	RpaymentRecordId4
paymentRecord (2.PaymentRecordRpaymentRecord"⁄
AckPaymentSent
id (	Rid&
conversationId (	RconversationId
status (Rstatus
message (	Rmessage(
paymentRecordId (	RpaymentRecordId4
paymentRecord (2.PaymentRecordRpaymentRecord"Ê
PaymentStatus
id (	Rid
	reference (	R	reference
status (	Rstatus
message (	Rmessage
fee (	Rfee*
ackPaymentSentId (	RackPaymentSentId7
ackPaymentSent (2.AckPaymentSentRackPaymentSent2Ç
PaymentProviderService3
sendPayment.SendPaymentRequest.AckPaymentSent3
getPaymentStatus.AckPaymentSent.PaymentStatus2L
SendPaymentRecordService0
remoteProcess.PaymentRecord.AckPaymentSent2P
ProcessAckPaymentSentService0
remoteProcess.AckPaymentSent.PaymentStatus2M
PollAckPaymentSentService0
remoteProcess.AckPaymentSent.PaymentStatusB 
org.pipelineframework.csv.grpcbproto3
ù
payment_status_svc.protopayments_processing_svc.proto"Á
PaymentOutput
id (	Rid<
csvPaymentsOutputFilename (	RcsvPaymentsOutputFilename
csvId (	RcsvId
	recipient (	R	recipient
amount (	Ramount
currency (	Rcurrency&
conversationId (	RconversationId
status (Rstatus
message	 (	Rmessage
fee
 (	Rfee4
paymentStatus (2.PaymentStatusRpaymentStatus2N
ProcessPaymentStatusService/
remoteProcess.PaymentStatus.PaymentOutputB 
org.pipelineframework.csv.grpcbproto3
¡
orchestrator.protoorchestrator"8
ProcessRequest&
csv_folder_path (	RcsvFolderPath"E
ProcessResponse
success (Rsuccess
message (	Rmessage2]
OrchestratorServiceF
Process.orchestrator.ProcessRequest.orchestrator.ProcessResponseB5
org.pipelineframework.csv.grpcBOrchestratorProtoPbproto3
π
$output_csv_file_processing_svc.protopayment_status_svc.proto"i
CsvPaymentsOutputFile
id (	Rid
filepath (	Rfilepath$
csvFolderPath (	RcsvFolderPath2b
#ProcessCsvPaymentsOutputFileService;
remoteProcess.PaymentOutput.CsvPaymentsOutputFile(0B 
org.pipelineframework.csv.grpcbproto3