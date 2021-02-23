_async架构图_

![Image text](https://github.com/xuliang12/async-test/blob/master/asyncframework.png)

传统的Future、CompleteableFuture一定程度上可以完成任务编排，并可以把结果传递到下一个任务。如CompletableFuture有then方法，但是却无法做到对每一个执行单元的回调。譬如A执行完毕成功了，后面是B，我希望A在执行完后就有个回调结果，方便我监控当前的执行状况，或者打个日志什么的。失败了，我也可以记录个异常信息什么的。

此时，CompleteableFuture就无能为力了。

我的框架提供了这样的回调功能。并且，如果执行异常、超时，可以在定义这个执行单元时就设定默认值。