///
/// Sorts the given data buffer

__kernel void sort(const unsigned int size, __global int* data) {
    int index = get_global_id(0);
    int before_index = (index -1 <= 0)? 0: index - 1;
    int after_index = (index +1 >= size)? size-1: index + 1;
    for (int round = 0; round < size; round++) {
        int me = data[index];
        int before = data[before_index];
        int after = data[after_index];
        barrier(CLK_GLOBAL_MEM_FENCE); // to synchronize
        if (((round+index) %2) == 0) {
            if (me < after) {
                data[index] = after;
            }
        } else {
            if (me >= before) {
                data[index] = before;
            }
        }
        barrier(CLK_GLOBAL_MEM_FENCE); // to synchronize
    }
}
