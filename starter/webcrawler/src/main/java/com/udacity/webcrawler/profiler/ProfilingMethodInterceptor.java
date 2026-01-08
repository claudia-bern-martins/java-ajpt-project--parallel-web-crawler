package com.udacity.webcrawler.profiler;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * A method interceptor that checks whether {@link Method}s are annotated with the {@link Profiled}
 * annotation. If they are, the method interceptor records how long the method invocation took.
 */
final class ProfilingMethodInterceptor implements InvocationHandler {

  private final Clock clock;
  private final Object targetObject;
  private final ProfilingState profilingState;

  ProfilingMethodInterceptor(Clock clock, Object targetObject,
                             ProfilingState profilingState) {
    this.clock = Objects.requireNonNull(clock);
    this.targetObject = Objects.requireNonNull(targetObject);
    this.profilingState = Objects.requireNonNull(profilingState);
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      Instant beginning = clock.instant();
      try {
          if (method.getDeclaringClass().equals(Object.class) && method.getName().equals("equals")) {
              return targetObject.equals(args[0]);
          }
          return method.invoke(targetObject, args);
      } catch (InvocationTargetException e) {
          throw e.getTargetException();
      } catch (IllegalAccessException e) {
          throw new RuntimeException(e);
      } finally {
          if(method.isAnnotationPresent(Profiled.class)) {
              Instant ending = clock.instant();
              Duration duration = Duration.between(beginning, ending);
              profilingState.record(targetObject.getClass(), method, duration);
          }
      }
  }
}
