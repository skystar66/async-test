package com.xuliang.framework.async.worker;


import lombok.Data;

/**
 * 执行结果
 *
 * @author xuliang
 */
@Data
public class WorkResult<V> {


    /**
     * 执行结果
     */
    private V result;

    /**
     * 结果状态
     */
    private ResultState resultState;

    /**
     * 异常信息
     */
    private Exception ex;


    public WorkResult(V result, ResultState resultState) {
        this(result, resultState, null);
    }

    public WorkResult(V result, ResultState resultState, Exception ex) {
        this.result = result;
        this.resultState = resultState;
        this.ex = ex;
    }

    public static <V> WorkResult<V> defaultResult() {
        return new WorkResult<>(null, ResultState.DEFAULT);
    }

}
