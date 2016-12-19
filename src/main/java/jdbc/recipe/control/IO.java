package jdbc.recipe.control;

import javaslang.CheckedFunction0;
import javaslang.Function1;

public interface IO<T> {

    T run();
    <U> IO<U> flatMap(Function1<T, IO<U>> func);

    static <T> IO<T> of(CheckedFunction0<T> f) {
        return new IOImpl<T>(f);
    }

    class IOImpl<T> implements IO<T> {
        private final CheckedFunction0<T> f;

        IOImpl(CheckedFunction0<T> f) {
            this.f = f.memoized();
        }

        @Override
        public T run() {
            try {
                return f.apply();
            } catch (Throwable throwable) {
                throw new RuntimeException(throwable);
            }
        }

        @Override
        public <U> IO<U> flatMap(Function1<T, IO<U>> func) {
            return new IOImpl<U>(() -> func.apply(f.apply()).run());
        }

    }
}
