provider "aws" {
  region = var.aws_region
}

locals {
  base_environment = {
    QUARKUS_PROFILE                              = "lambda-modular"
    # This manual example leans on CloudWatch/Lambda metrics instead of in-process exporters.
    QUARKUS_OTEL_ENABLED                         = "false"
    QUARKUS_MICROMETER_EXPORT_PROMETHEUS_ENABLED = "false"
  }

  step_functions = {
    crawl-source-svc = {
      zip_path = "${path.root}/${var.crawl_source_zip_path}"
      timeout  = 60
    }
    parse-document-svc = {
      zip_path = "${path.root}/${var.parse_document_zip_path}"
      timeout  = 60
    }
    tokenize-content-svc = {
      zip_path = "${path.root}/${var.tokenize_content_zip_path}"
      timeout  = 60
    }
    index-document-svc = {
      zip_path = "${path.root}/${var.index_document_zip_path}"
      timeout  = 60
    }
  }

  orchestrator_zip_path = "${path.root}/${var.orchestrator_zip_path}"
}

data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda_exec" {
  name               = "${var.name_prefix}-lambda-exec"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

resource "aws_iam_role_policy_attachment" "basic_execution" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_lambda_function" "step" {
  for_each = local.step_functions

  function_name    = "${var.name_prefix}-${each.key}"
  role             = aws_iam_role.lambda_exec.arn
  filename         = each.value.zip_path
  source_code_hash = filebase64sha256(each.value.zip_path)
  runtime          = "java21"
  handler          = "io.quarkus.amazon.lambda.runtime.QuarkusStreamHandler::handleRequest"
  architectures    = ["x86_64"]
  memory_size      = 1024
  timeout          = each.value.timeout

  environment {
    variables = local.base_environment
  }

  depends_on = [aws_iam_role_policy_attachment.basic_execution]
}

resource "aws_lambda_function_url" "step" {
  for_each = aws_lambda_function.step

  function_name      = each.value.function_name
  # Public Function URLs are intentional for the disposable manual example workflow.
  authorization_type = "NONE"
  invoke_mode        = "BUFFERED"
}

resource "aws_lambda_permission" "step_function_url_invoke" {
  for_each = aws_lambda_function.step

  # Keep the explicit invoke permission for the public Function URL example path.
  statement_id  = "AllowPublicFunctionInvoke"
  action        = "lambda:InvokeFunction"
  function_name = each.value.function_name
  principal     = "*"
}

resource "aws_lambda_function" "orchestrator" {
  function_name    = "${var.name_prefix}-orchestrator-svc"
  role             = aws_iam_role.lambda_exec.arn
  filename         = local.orchestrator_zip_path
  source_code_hash = filebase64sha256(local.orchestrator_zip_path)
  runtime          = "java21"
  handler          = "io.quarkus.amazon.lambda.runtime.QuarkusStreamHandler::handleRequest"
  architectures    = ["x86_64"]
  memory_size      = 1536
  timeout          = 120

  environment {
    variables = merge(local.base_environment, {
      # The first modular AWS slice keeps cache state in-memory to avoid external infrastructure.
      PIPELINE_CACHE_PROVIDER                          = "memory"
      QUARKUS_REST_CLIENT_PROCESS_CRAWL_SOURCE_URL     = aws_lambda_function_url.step["crawl-source-svc"].function_url
      QUARKUS_REST_CLIENT_PROCESS_PARSE_DOCUMENT_URL   = aws_lambda_function_url.step["parse-document-svc"].function_url
      QUARKUS_REST_CLIENT_PROCESS_TOKENIZE_CONTENT_URL = aws_lambda_function_url.step["tokenize-content-svc"].function_url
      QUARKUS_REST_CLIENT_PROCESS_INDEX_DOCUMENT_URL   = aws_lambda_function_url.step["index-document-svc"].function_url
    })
  }

  depends_on = [aws_iam_role_policy_attachment.basic_execution]
}

resource "aws_lambda_function_url" "orchestrator" {
  function_name      = aws_lambda_function.orchestrator.function_name
  # Public Function URLs are intentional for the disposable manual example workflow.
  authorization_type = "NONE"
  invoke_mode        = "BUFFERED"
}

resource "aws_lambda_permission" "orchestrator_function_url_invoke" {
  # Keep the explicit invoke permission for the public Function URL example path.
  statement_id  = "AllowPublicFunctionInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.orchestrator.function_name
  principal     = "*"
}
