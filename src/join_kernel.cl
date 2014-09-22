#define MAX_HASH_VALUE (1<<12) 

/// A crude and naive hash implementation
__kernel void naive_hash(const int val, int * hahs_vlaue) {
    *hahs_vlaue = (val * 8191) % MAX_HASH_VALUE; // other values to use: 127, 4047, 8191
}
__kernel void hash_join( __global int* probe_column, const unsigned int probe_count, __global int* build_column, const unsigned int build_count, __global char* join_result)
{
    int thread_idx = get_global_id(0);
    join_result[thread_idx] = build_column[thread_idx] == probe_column[thread_idx];
}

/// Assumes the data size is the same as number of threads launched.
/// This is the first step of hashing
__kernel void hash(__global int* data, __global int* hashed_data) {
    int thread_idx = get_global_id(0);
    int hash_value = 0;
    naive_hash(data[thread_idx], &hash_value);
    hashed_data[thread_idx] = hash_value;
}

__kernel void count_hash_values(__global int* hashed_data, __global int* counts){
    int thread_idx = get_global_id(0);
    counts[thread_idx] = 0;
    int my_value = hashed_data[thread_idx];
    atomic_inc(&counts[my_value]);
}

/// Asumes that size of data is same as numbeur of threads launched.
__kernel void prefix_sum(__global int* counts, __global int* prefix_sum) {
    int thread_idx = get_global_id(0);
    int n = get_global_size(0);
    prefix_sum[thread_idx] = (thread_idx > 0)? counts[thread_idx-1]: 0;
    int stride = 0;
    for (stride = 1; stride < n; stride <<=  1){
        if (thread_idx >= stride) {
            prefix_sum[thread_idx]  += prefix_sum[thread_idx - (stride>>1)];
        }
        barrier(CLK_GLOBAL_MEM_FENCE);
    }  
}
