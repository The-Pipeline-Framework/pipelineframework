
Õ
crawl-source-svc.proto"B
CrawlRequest
docId (	RdocId
	sourceUrl (	R	sourceUrl"
RawDocument
docId (	RdocId
	sourceUrl (	R	sourceUrl

rawContent (	R
rawContent
	fetchedAt (	R	fetchedAt2I
ProcessCrawlSourceService,
remoteProcess.CrawlRequest.RawDocumentB#
!org.pipelineframework.search.grpcbproto3
¨
parse-document-svc.protocrawl-source-svc.proto"x
ParsedDocument
docId (	RdocId
title (	Rtitle
content (	Rcontent 
extractedAt (	RextractedAt2M
ProcessParseDocumentService.
remoteProcess.RawDocument.ParsedDocumentB#
!org.pipelineframework.search.grpcbproto3
‘
tokenize-content-svc.protoparse-document-svc.proto"\

TokenBatch
docId (	RdocId
tokens (	Rtokens 
tokenizedAt (	RtokenizedAt2N
ProcessTokenizeContentService-
remoteProcess.ParsedDocument.TokenBatchB#
!org.pipelineframework.search.grpcbproto3
©
index-document-svc.prototokenize-content-svc.proto"|
IndexAck
docId (	RdocId"
indexVersion (	RindexVersion
	indexedAt (	R	indexedAt
success (Rsuccess2F
ProcessIndexDocumentService'
remoteProcess.TokenBatch	.IndexAckB#
!org.pipelineframework.search.grpcbproto3