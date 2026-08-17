package org.pipelineframework.protocol;

import java.util.Collection;

/**
 * Build-time contribution seam for portable protocol vocabulary shipped by a framework extension.
 *
 * <p>Contributions describe only canonical v3 semantic types. They do not provide runtime behavior,
 * external schemas, credentials, or provider configuration.</p>
 */
public interface ProtocolTypeContributor {
    Collection<ProtocolTypeDescriptor> protocolTypes();
}
