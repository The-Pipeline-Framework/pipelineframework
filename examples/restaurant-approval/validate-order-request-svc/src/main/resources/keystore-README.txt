# Keystore File Needed
#
# This application requires a server-keystore.jks file for SSL/TLS functionality.
#
# Please generate or obtain a keystore file and place it in this location:
#
# /Users/mari/tpf3/tmp-generated-restaurant/validate-order-request-svc/src/main/resources/server-keystore.jks
#
# For development purposes, you can create a self-signed certificate using:
# keytool -genkey -alias server -keyalg RSA -keystore server-keystore.jks -storetype PKCS12
#
# For production, please use a proper certificate from a trusted CA.
