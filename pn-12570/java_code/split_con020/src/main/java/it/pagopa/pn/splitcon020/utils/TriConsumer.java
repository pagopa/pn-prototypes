package it.pagopa.pn.splitcon020.utils;

@FunctionalInterface
public interface TriConsumer<A, B, C> {

     void accept( A a, B b, C c);
}
