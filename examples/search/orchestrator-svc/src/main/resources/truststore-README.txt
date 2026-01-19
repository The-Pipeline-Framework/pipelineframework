# Truststore File Needed
#
# This application requires a client-truststore.jks file for SSL/TLS client functionality.
#
# Please generate or obtain a truststore file and place it in this location:
#
# /Users/mari/IdeaProjects/pipelineframework/examples/search/target/dev-certs/orchestrator-svc/client-truststore.jks
#
# For development purposes, you can create a truststore using:
# keytool -import -alias server -file server-cert.pem -keystore client-truststore.jks
#
# For production, please use a proper certificate from a trusted CA.
