output "orchestrator_function_url" {
  description = "Public Function URL for the Search orchestrator."
  value       = aws_lambda_function_url.orchestrator.function_url
}

output "crawl_source_function_url" {
  description = "Public Function URL for crawl-source-svc."
  value       = aws_lambda_function_url.step["crawl-source-svc"].function_url
}

output "parse_document_function_url" {
  description = "Public Function URL for parse-document-svc."
  value       = aws_lambda_function_url.step["parse-document-svc"].function_url
}

output "tokenize_content_function_url" {
  description = "Public Function URL for tokenize-content-svc."
  value       = aws_lambda_function_url.step["tokenize-content-svc"].function_url
}

output "index_document_function_url" {
  description = "Public Function URL for index-document-svc."
  value       = aws_lambda_function_url.step["index-document-svc"].function_url
}
