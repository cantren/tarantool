--TEST--
Tarantool/box call commands test
--FILE--
<?php
include "lib/php/tarantool_utest.php";

$tarantool = new Tarantool("localhost", 33013, 33015);
test_init($tarantool, 0);

echo "---------- test begin ----------\n";
echo "test call: myselect by primary index\n";
test_call($tarantool, "box.select", array(0, 0, 2), 0);
echo "----------- test end -----------\n\n";

echo "---------- test begin ----------\n";
echo "test call: call undefined function (expected error exception)\n";
test_call($tarantool, "fafagaga", array("fafa-gaga", "foo", "bar"), 0);
echo "----------- test end -----------\n\n";

echo "---------- test begin ----------\n";
echo "test call: sa_insert to key_1\n";
test_call($tarantool, "box.sa_insert", array("1", "key_1", "10"), 0);
test_call($tarantool, "box.sa_insert", array("1", "key_1", "11"), 0);
test_call($tarantool, "box.sa_insert", array("1", "key_1", "15"), 0);
test_call($tarantool, "box.sa_insert", array("1", "key_1", "101"), 0);
test_call($tarantool, "box.sa_insert", array("1", "key_1", "511"), 0);
test_call($tarantool, "box.sa_insert", array("1", "key_1", "16"), 0);
test_call($tarantool, "box.sa_insert", array("1", "key_1", "42"), 0);
echo "----------- test end -----------\n\n";

echo "---------- test begin ----------\n";
echo "test call: sa_select from key_1\n";
test_call($tarantool, "box.sa_select", array("1", "key_1", "100", "3"), 0);
test_call($tarantool, "box.sa_select", array("1", "key_1", "100", "2"), 0);
test_call($tarantool, "box.sa_select", array("1", "key_1", "511", "4"), 0);
echo "----------- test end -----------\n\n";

echo "---------- test begin ----------\n";
echo "test call: sa_insert to key_2\n";
test_call($tarantool, "box.sa_insert", array("1", "key_2", "10"), 0);
test_call($tarantool, "box.sa_insert", array("1", "key_2", "8"), 0);
test_call($tarantool, "box.sa_insert", array("1", "key_2", "500"), 0);
test_call($tarantool, "box.sa_insert", array("1", "key_2", "166"), 0);
test_call($tarantool, "box.sa_insert", array("1", "key_2", "233"), 0);
test_call($tarantool, "box.sa_insert", array("1", "key_2", "357"), 0);
echo "----------- test end -----------\n\n";

echo "---------- test begin ----------\n";
echo "test call: sa_select from key_2\n";
test_call($tarantool, "box.sa_select", array("1", "key_1", "500", "100"), 0);
test_call($tarantool, "box.sa_select", array("1", "key_1", "18", "15"), 0);
test_call($tarantool, "box.sa_select", array("1", "key_1", "18", "1"), 0);
echo "----------- test end -----------\n\n";

echo "---------- test begin ----------\n";
echo "test call: sa_merge key_1 and key_2\n";
test_call($tarantool, "box.sa_merge", array("1", "key_1", "key_2"), 0);
echo "----------- test end -----------\n\n";

test_clean($tarantool, 0);
?>
===DONE===
--EXPECT--
---------- test begin ----------
test call: myselect by primary index
result:
array(2) {
  ["count"]=>
  int(1)
  ["tuples_list"]=>
  array(1) {
    [0]=>
    array(6) {
      [0]=>
      int(2)
      [1]=>
      string(9) "Star Wars"
      [2]=>
      int(1983)
      [3]=>
      string(18) "Return of the Jedi"
      [4]=>
      string(460) "Luke Skywalker has returned
to his home planet of
Tatooine in an attempt
to rescue his friend
Han Solo from the
clutches of the vile
gangster Jabba the Hutt.

Little does Luke know
that the GALACTIC EMPIRE
has secretly begun construction
on a new armored space station
even more powerful than the
first dreaded Death Star.

When completed, this ultimate
weapon will spell certain
doom for the small band of
rebels struggling to restore
freedom to the galaxy..."
      [5]=>
      int(-1091633149)
    }
  }
}
----------- test end -----------

---------- test begin ----------
test call: call undefined function (expected error exception)
catched exception: call failed: 12802(0x00003202): Procedure 'fafagaga' is not defined
----------- test end -----------

---------- test begin ----------
test call: sa_insert to key_1
result:
array(2) {
  ["count"]=>
  int(1)
  ["tuples_list"]=>
  array(1) {
    [0]=>
    array(2) {
      [0]=>
      string(5) "key_1"
      [1]=>
      int(0)
    }
  }
}
result:
array(2) {
  ["count"]=>
  int(1)
  ["tuples_list"]=>
  array(1) {
    [0]=>
    array(2) {
      [0]=>
      string(5) "key_1"
      [1]=>
      string(16) "       
       "
    }
  }
}
result:
array(2) {
  ["count"]=>
  int(1)
  ["tuples_list"]=>
  array(1) {
    [0]=>
    array(2) {
      [0]=>
      string(5) "key_1"
      [1]=>
      string(24) "              
       "
    }
  }
}
result:
array(2) {
  ["count"]=>
  int(1)
  ["tuples_list"]=>
  array(1) {
    [0]=>
    array(2) {
      [0]=>
      string(5) "key_1"
      [1]=>
      string(32) "e                     
       "
    }
  }
}
result:
array(2) {
  ["count"]=>
  int(1)
  ["tuples_list"]=>
  array(1) {
    [0]=>
    array(2) {
      [0]=>
      string(5) "key_1"
      [1]=>
      string(40) "�      e                     
       "
    }
  }
}
result:
array(2) {
  ["count"]=>
  int(1)
  ["tuples_list"]=>
  array(1) {
    [0]=>
    array(2) {
      [0]=>
      string(5) "key_1"
      [1]=>
      string(48) "�      e                            
       "
    }
  }
}
result:
array(2) {
  ["count"]=>
  int(1)
  ["tuples_list"]=>
  array(1) {
    [0]=>
    array(2) {
      [0]=>
      string(5) "key_1"
      [1]=>
      string(56) "�      e       *                            
       "
    }
  }
}
----------- test end -----------

---------- test begin ----------
test call: sa_select from key_1
result:
array(2) {
  ["count"]=>
  int(2)
  ["tuples_list"]=>
  array(2) {
    [0]=>
    array(1) {
      [0]=>
      string(5) "key_1"
    }
    [1]=>
    array(3) {
      [0]=>
      string(2) "42"
      [1]=>
      string(2) "16"
      [2]=>
      string(2) "15"
    }
  }
}
result:
array(2) {
  ["count"]=>
  int(2)
  ["tuples_list"]=>
  array(2) {
    [0]=>
    array(1) {
      [0]=>
      string(5) "key_1"
    }
    [1]=>
    array(2) {
      [0]=>
      string(2) "42"
      [1]=>
      string(2) "16"
    }
  }
}
result:
array(2) {
  ["count"]=>
  int(2)
  ["tuples_list"]=>
  array(2) {
    [0]=>
    array(1) {
      [0]=>
      string(5) "key_1"
    }
    [1]=>
    array(4) {
      [0]=>
      string(3) "101"
      [1]=>
      string(2) "42"
      [2]=>
      string(2) "16"
      [3]=>
      string(2) "15"
    }
  }
}
----------- test end -----------

---------- test begin ----------
test call: sa_insert to key_2
result:
array(2) {
  ["count"]=>
  int(1)
  ["tuples_list"]=>
  array(1) {
    [0]=>
    array(2) {
      [0]=>
      string(5) "key_2"
      [1]=>
      int(0)
    }
  }
}
result:
array(2) {
  ["count"]=>
  int(1)
  ["tuples_list"]=>
  array(1) {
    [0]=>
    array(2) {
      [0]=>
      string(5) "key_2"
      [1]=>
      string(16) "
              "
    }
  }
}
result:
array(2) {
  ["count"]=>
  int(1)
  ["tuples_list"]=>
  array(1) {
    [0]=>
    array(2) {
      [0]=>
      string(5) "key_2"
      [1]=>
      string(24) "�      
              "
    }
  }
}
result:
array(2) {
  ["count"]=>
  int(1)
  ["tuples_list"]=>
  array(1) {
    [0]=>
    array(2) {
      [0]=>
      string(5) "key_2"
      [1]=>
      string(32) "�      �       
              "
    }
  }
}
result:
array(2) {
  ["count"]=>
  int(1)
  ["tuples_list"]=>
  array(1) {
    [0]=>
    array(2) {
      [0]=>
      string(5) "key_2"
      [1]=>
      string(40) "�      �       �       
              "
    }
  }
}
result:
array(2) {
  ["count"]=>
  int(1)
  ["tuples_list"]=>
  array(1) {
    [0]=>
    array(2) {
      [0]=>
      string(5) "key_2"
      [1]=>
      string(48) "�      e      �       �       
              "
    }
  }
}
----------- test end -----------

---------- test begin ----------
test call: sa_select from key_2
result:
array(2) {
  ["count"]=>
  int(2)
  ["tuples_list"]=>
  array(2) {
    [0]=>
    array(1) {
      [0]=>
      string(5) "key_1"
    }
    [1]=>
    array(6) {
      [0]=>
      string(3) "101"
      [1]=>
      string(2) "42"
      [2]=>
      string(2) "16"
      [3]=>
      string(2) "15"
      [4]=>
      string(2) "11"
      [5]=>
      string(2) "10"
    }
  }
}
result:
array(2) {
  ["count"]=>
  int(2)
  ["tuples_list"]=>
  array(2) {
    [0]=>
    array(1) {
      [0]=>
      string(5) "key_1"
    }
    [1]=>
    array(4) {
      [0]=>
      string(2) "16"
      [1]=>
      string(2) "15"
      [2]=>
      string(2) "11"
      [3]=>
      string(2) "10"
    }
  }
}
result:
array(2) {
  ["count"]=>
  int(2)
  ["tuples_list"]=>
  array(2) {
    [0]=>
    array(1) {
      [0]=>
      string(5) "key_1"
    }
    [1]=>
    array(1) {
      [0]=>
      string(2) "16"
    }
  }
}
----------- test end -----------

---------- test begin ----------
test call: sa_merge key_1 and key_2
result:
array(2) {
  ["count"]=>
  int(1)
  ["tuples_list"]=>
  array(1) {
    [0]=>
    array(13) {
      [0]=>
      string(3) "511"
      [1]=>
      string(3) "500"
      [2]=>
      string(3) "357"
      [3]=>
      string(3) "233"
      [4]=>
      string(3) "166"
      [5]=>
      string(3) "101"
      [6]=>
      string(2) "42"
      [7]=>
      string(2) "16"
      [8]=>
      string(2) "15"
      [9]=>
      string(2) "11"
      [10]=>
      string(2) "10"
      [11]=>
      string(2) "10"
      [12]=>
      string(1) "8"
    }
  }
}
----------- test end -----------

===DONE===