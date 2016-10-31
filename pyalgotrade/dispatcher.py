# coding=utf-8
# PyAlgoTrade
#
# Copyright 2011-2015 Gabriel Martin Becedillas Ruiz
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
.. moduleauthor:: Gabriel Martin Becedillas Ruiz <gabriel.becedillas@gmail.com>
"""

from pyalgotrade import utils
from pyalgotrade import observer
from pyalgotrade import dispatchprio


# This class is responsible for dispatching events from multiple subjects, synchronizing them if necessary.

#分发多个事件源来的事件，驱动所有的功能（or 代码）
class Dispatcher(object):
    def __init__(self):
        self.__subjects = [] #事件源
        self.__stop = False #是否要停止整个运行了
        self.__startEvent = observer.Event()
        self.__idleEvent = observer.Event()
        self.__currDateTime = None #当前时间，精确到秒？

    # Returns the current event datetime. It may be None for events from realtime subjects.
    def getCurrentDateTime(self):
        return self.__currDateTime

    def getStartEvent(self):
        return self.__startEvent

    def getIdleEvent(self):
        return self.__idleEvent

    def stop(self):
        self.__stop = True

    def getSubjects(self):
        return self.__subjects

    def addSubject(self, subject):
        # Skip the subject if it was already added.
        if subject in self.__subjects:
            return

        # If the subject has no specific dispatch priority put it right at the end.
        if subject.getDispatchPriority() is dispatchprio.LAST:
            self.__subjects.append(subject)
        else:
            # Find the position according to the subject's priority.
            pos = 0
            for s in self.__subjects:
                if s.getDispatchPriority() is dispatchprio.LAST or subject.getDispatchPriority() < s.getDispatchPriority():
                    break
                pos += 1
            self.__subjects.insert(pos, subject)

        subject.onDispatcherRegistered(self) #回调一下事件源，告知成功加入分发bus

    # Return True if events were dispatched.
    def __dispatchSubject(self, subject, currEventDateTime):
        ret = False
        # Dispatch if the datetime is currEventDateTime of if its a realtime subject.
        if not subject.eof() and subject.peekDateTime() in (None, currEventDateTime):
            ret = subject.dispatch() is True
        return ret

    # Returns a tuple with booleans
    # 1: True if all subjects hit eof
    # 2: True if at least one subject dispatched events.
    def __dispatch(self):
        smallestDateTime = None #
        eof = True
        eventsDispatched = False

        # Scan for the lowest datetime.
        for subject in self.__subjects:
            if not subject.eof():
                eof = False
                smallestDateTime = utils.safe_min(smallestDateTime, subject.peekDateTime())

        #一次只返回多个事件源中最老的事件，这是为了保证处理的顺序没有问题。比如过2分钟采取查看，有两个事件源有事件，一个发生在第1分钟，一个1分半
        #那么会先取第一分钟发生的事件吐出去
        # Dispatch realtime subjects and those subjects with the lowest datetime.
        if not eof:
            self.__currDateTime = smallestDateTime

            for subject in self.__subjects:
                if self.__dispatchSubject(subject, smallestDateTime):
                    eventsDispatched = True
        return eof, eventsDispatched

    def run(self):
        try:
            #触发每个事件源都启动起来，比如说futunn开始接收数据了，如果是分钟线的策略，那么先加载一下今天从开盘到现在所有分钟k线
            for subject in self.__subjects:
                subject.start()

            #监听dispath启动的所有函数都可以收到消息
            self.__startEvent.emit()

            while not self.__stop:
                eof, eventsDispatched = self.__dispatch()
                if eof: #如果是csv的回测数据，那么这里就结束了，可以开始计算回测结果啥的了
                    self.__stop = True
                elif not eventsDispatched:
                    self.__idleEvent.emit() #没有事件过来，可以做一些见缝插针做的事情，比如说内存清理等
        finally:
            for subject in self.__subjects:
                subject.stop()
            for subject in self.__subjects:
                subject.join() #
