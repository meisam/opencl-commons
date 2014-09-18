__kernel void hash_join( __global int* probe_column, const unsigned int probe_count, __global int* build_column, const unsigned int build_count, __global char* join_result)
{
    int thread_idx = get_global_id(0);
    join_result[thread_idx] = build_column[thread_idx] == probe_column[thread_idx];
}
