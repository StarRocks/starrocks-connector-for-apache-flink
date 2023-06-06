package com.starrocks.data.load.stream;

import java.util.List;

public interface CommitListener {
    public boolean afterCommit(List<Meta> metaDatas);
}


