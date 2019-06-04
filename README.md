# 简单的日志处理框架

## 功能

- 跟踪文件变化，将读取的行提交给回调的方法做解析，然后推送到elasticsearch或者其他输出
- 包含一个sqlite数据库文件，用于继续上次的扫描
- 能够检测文件inode的变化，停止扫描时间不更新的文件

## 用法

### 自定义parser

需要继承Logparser类并重写parser方法

- 每次日志读取了一行，parser就会被调用，进行解析处理
- parser可以使用实例的context变量存储会话中的数据
- 这个方法的返回值会被推到队列中，交给下个流程


### 定义一个logSlice实例

#### LogSlice

args:

- path: 需要扫描的日志的路径. glob格式的字符串或者列表。 例如\'/var/log/\*.log\', \'/var/\*\*/*.log\', [\'/var/log/\*.log\', \'/data/\*\*/\*.txt\']
- output: 用于输出的可调用对象，接受一个可迭代对象作为输入参数。每到队列达到缓存时，调用output方法
- dbpath: 数据库的目录, 默认为/var/lib/logslice
- parser: 用于做解析的LogParser类
- file_filter: 正则表达式过滤器，用于排除某些不想扫描的文件
- encoding: 文件编码，默认为utf-8
- rescan_interval: 多久重新扫描一次文件， 默认为30秒
- close_file_time: 多久没有更新后关闭文件， 默认为30秒


例子:

```python
from logslice import LogParser, LogSlice, EchoOutPut


class MyParser(LogParser):
    
    def parser(self, line):
        if self.context is None:
            self.context = []
        if len(self.context) < 2:
            self.context.append(line)
        else:
            ret = ','.join(self.context)
            self.context.clear()
            return ret


logslice = LogSlice(path='/var/log/*.log', output=EchoOutPut, dbpath='/data/logslice', parser=MyParser)
logslice.start()
```


### OutPut

#### EchoOutPut

打印数据，用于调试

#### ElasticOutPut

将数据发送到ElasticSearch

- 如果parser结果返回的是字典对象， 那么取字典对象中的@timestamp的值作为时间戳，如果无法取到，则取当前时间。
- 如果parser返回的是其他类型，则将整个数据转换成str类型，取当前时间作为时间戳

args:

- index: 想要推送的索引的名称， 可以包含timeformat的标志，会根据具体时间进行解析，例如someindex-%Y%m%d
- doc_type: es的日志类型，默认为doc
- 其他参数会全部传给ElasticSearch，可以参考文档[elasticsearch-py官方文档](https://elasticsearch-py.readthedocs.io/en/master/)
