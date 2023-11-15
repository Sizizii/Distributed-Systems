# Run each line of command in a separated terminal.
# And please modify the var LOCALHOST to your local ip address.

# 3 Nodes
python -u gentx.py 0.5 | python node.py node1 1201 config1.txt
python -u gentx.py 0.5 | python node.py node2 1202 config2.txt
python -u gentx.py 0.5 | python node.py node3 1203 config3.txt

# 8 Nodes. Run about 110s for 100s input test and 210s for 200s input test.
python -u gentx.py 5 | python node.py node1 1201 config8_1.txt
python -u gentx.py 5 | python node.py node2 1202 config8_2.txt
python -u gentx.py 5 | python node.py node3 1203 config8_3.txt
python -u gentx.py 5 | python node.py node4 1204 config8_4.txt
python -u gentx.py 5 | python node.py node5 1205 config8_5.txt
python -u gentx.py 5 | python node.py node6 1206 config8_6.txt
python -u gentx.py 5 | python node.py node7 1207 config8_7.txt
python -u gentx.py 5 | python node.py node8 1208 config8_8.txt