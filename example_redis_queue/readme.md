# Redis队列

这里会用到两个非常有用的命令

`rbpoplpush a_list b_list` - 把a_list最右边的元素加到b最左边

`lrem a_list count element` - 从a_list里面移除值为element的元素

