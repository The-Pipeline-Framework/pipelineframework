package org.pipelineframework.processor.phase;

import javax.annotation.processing.Messager;
import javax.tools.Diagnostic;

/**
 * A no-op implementation of Messager that discards all messages.
 * Used when processing environment is not available.
 */
class NoOpMessager implements Messager {

    @Override
    public void printMessage(Diagnostic.Kind kind, CharSequence msg) {
        // Do nothing
    }

    @Override
    public void printMessage(Diagnostic.Kind kind, CharSequence msg, javax.lang.model.element.Element e) {
        // Do nothing
    }

    @Override
    public void printMessage(Diagnostic.Kind kind, CharSequence msg, javax.lang.model.element.Element e, javax.lang.model.element.AnnotationMirror a) {
        // Do nothing
    }

    @Override
    public void printMessage(Diagnostic.Kind kind, CharSequence msg, javax.lang.model.element.Element e, javax.lang.model.element.AnnotationMirror a, javax.lang.model.element.AnnotationValue v) {
        // Do nothing
    }
}