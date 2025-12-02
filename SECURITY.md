# Security Policy

## Reporting a Vulnerability

We take security vulnerabilities seriously and appreciate your efforts to responsibly disclose them.

To report a security vulnerability, please contact us directly at [security@pipelineframework.org]
(mailto:security@pipelineframework.org) or [team@pipelineframework.org](mailto:team@pipelineframework.org).

**Please do not report security vulnerabilities through public GitHub issues.**

### Information to Include

When reporting a security vulnerability, please include:

- A detailed description of the vulnerability
- Steps to reproduce the issue
- Affected versions
- Potential impact
- Any possible mitigations you've identified

### What to Expect

After submitting a security report:

1. You will receive an acknowledgment within 48 hours
2. We will investigate and respond with next steps within 5 business days
3. We will keep you informed of our progress toward a fix
4. Once addressed, we will publicly acknowledge your responsible disclosure (unless you prefer to remain anonymous)

## Security Updates

Security updates will be released as quickly as possible after a vulnerability is identified and a fix is developed. We recommend keeping your dependencies up to date to ensure you have the latest security patches.

## Dependencies

This project maintains its dependencies using Maven. We regularly update dependencies to include security patches when available. We monitor for known vulnerabilities in our dependencies through automated tools in our CI/CD pipeline.

## Version Support

| Version | Supported          |
| ------- | ------------------ |
| 0.9.x   | ✅ Latest releases |
| < 0.9   | ❌ Unsupported     |

Only the latest version series is actively maintained with security updates.