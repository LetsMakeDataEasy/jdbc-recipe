package jdbc.recipe.control;

import javaslang.Value;
import javaslang.collection.Iterator;
import javaslang.control.Option;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

public class IOValue<T> implements Value<IO<T>> {
    private final IO<T> io;

    public IOValue<T> of(IO<T> io) {
        return new IOValue<T>(io);
    }

    private IOValue(IO<T> io) {
        Objects.requireNonNull(io, "io should be non-null");
        this.io = io;
    }

    @Override
    public IO<T> get() {
        return this.io;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public boolean isSingleValued() {
        return true;
    }

    @Override
    public <U> Value<U> map(Function<? super IO<T>, ? extends U> mapper) {
        return null;
    }


    @Override
    public Value<IO<T>> peek(Consumer<? super IO<T>> action) {
        return null;
    }

    @Override
    public String stringPrefix() {
        return null;
    }

    @Override
    public Iterator<IO<T>> iterator() {
        return null;
    }
}
