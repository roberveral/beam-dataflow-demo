package com.github.roberveral.dataflow.transforms;

import com.google.cloud.dataflow.sdk.runners.PipelineRunner;

/**
 * Factory for getting the {@link PipelineRunner} class especified
 * by its class name.
 *
 * @author Roberto Veral
 */
public class PipelineRunnerFactory {
    /**
     * Obtains the {@link PipelineRunner} class especified by its class name.
     *
     * @param className class name of the {@link PipelineRunner} desdendant.
     * @return class especified.
     * @throws ClassNotFoundException if the given name doesn't match with any class
     */
    public static Class<? extends PipelineRunner<?>> obtain(String className) throws ClassNotFoundException {
        // Obtains class object for the given name
        Class<?> forName = Class.forName(className);
        /* Inheritance test (cast check) */
        if (PipelineRunner.class.isAssignableFrom(forName)) {
            return (Class<? extends PipelineRunner<?>>) forName;
        } else {
            throw new ClassCastException("The given class " + className + " is not a PipelineRunner");
        }
    }
}
