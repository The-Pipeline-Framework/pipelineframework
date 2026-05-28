# Keystore File Needed
#
# This application requires a server-keystore.p12 file for SSL/TLS functionality.
#
# Please generate or obtain a keystore file and place it in this location:
#
# src/main/resources/server-keystore.p12
#
# For development purposes, you can create a self-signed certificate using:
# keytool -genkey -alias server -keyalg RSA -keystore server-keystore.p12 -storetype PKCS12
#
# For production, please use a proper certificate from a trusted CA.
