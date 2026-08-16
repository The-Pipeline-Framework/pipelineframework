package org.pipelineframework.connector;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.spi.CreationalContext;
import jakarta.enterprise.inject.spi.Bean;
import jakarta.enterprise.inject.spi.BeanManager;
import jakarta.inject.Inject;

import io.quarkus.arc.ClientProxy;

/** Creates binding-owned, injected provider instances without exposing a provider-author factory SPI. */
@ApplicationScoped
final class QuarkusConnectorProviderInstanceFactory implements ConnectorProviderInstanceFactory {
    @Inject
    BeanManager beanManager;

    @Override
    public ConnectorProviderLease create(ConnectorProvider<?> prototype) {
        Bean<?> bean = prototype instanceof ClientProxy proxy
            ? proxy.arc_bean()
            : beanManager.resolve(beanManager.getBeans(prototype.getClass()));
        if (bean == null) {
            throw new IllegalStateException(
                "CDI connector provider bean is unavailable for " + prototype.getClass().getName());
        }
        return createInjected(bean);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private ConnectorProviderLease createInjected(Bean bean) {
        CreationalContext context = beanManager.createCreationalContext(bean);
        ConnectorProvider<?> provider;
        try {
            provider = (ConnectorProvider<?>) bean.create(context);
        } catch (RuntimeException | Error failure) {
            context.release();
            throw failure;
        }
        if (provider == null) {
            context.release();
            throw new IllegalStateException("CDI returned no connector provider instance for " + bean.getBeanClass().getName());
        }
        return ConnectorProviderLease.of(provider, () -> bean.destroy(provider, context));
    }
}
