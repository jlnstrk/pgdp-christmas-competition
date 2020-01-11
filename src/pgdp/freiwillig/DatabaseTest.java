package pgdp.freiwillig;


import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class DatabaseTest {

    @BeforeAll
    static void init() {
        Database.setBaseDataDirectory(Paths.get("data"));
    }

    @Test
    void integrity() {
        Database database = new Database();

        // first line
        assertTrue(database.customers.get("BUILDING".hashCode()).contains(1));

        // last line
        assertTrue(database.customers.get("AUTOMOBILE".hashCode()).contains(150000));

        // first line
        assertTrue(database.orders.get(36901).contains(1));

        // last line
        assertTrue(database.orders.get(110063).contains(6000000));
    }

}
