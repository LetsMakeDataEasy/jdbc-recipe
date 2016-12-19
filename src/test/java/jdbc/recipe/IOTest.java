package jdbc.recipe;

import jdbc.recipe.control.IO;
import org.junit.Test;

public class IOTest {

    IO<String> getString() {
        return IO.of(() -> "aabb");
    }

    IO<Void> printStr(String str) {
        return IO.of(() -> {
            System.out.println(str);
            return null;
        });
    }

    @Test
    public void test() {
        try {
            getString().flatMap(this::printStr).run();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }

}
