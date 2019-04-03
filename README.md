### Spark Installation and examples 

1. Update the manager and worker to your nodes' IP.
- Configure your 3-node cluster so that they can login without using password.
- Git clone the repository at the `root` directory
- Run ./insall.sh
- Run ./start.sh
- If you have `node-0 not recognized` error, please replace your the `node-0`, in the code, to your manager's IP address
- Run other examples


---

### RDD Examples

1. rdd-distinct: re-order the data
2. rdd-filter: apply filter to the data
3. rdd-flatmap: flatten the map data
4. rdd-kv: the key / value tests
5. rdd-map: map tests
6. rdd-maxmin: maximum and minimum test
7. rdd-union: union test
8. text-search: search on the text


### Spark Shell

1. file: file operations
2. list: list operations
3. max-count: count the maximum number of words a line 
4. sort: sort on data
5. words: word with maximum occurrences



---