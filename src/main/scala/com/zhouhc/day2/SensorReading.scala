package com.zhouhc.day2

//记录元素，也就是消息流中的一个元素
//id : 设备号 , timestamp : 系统时间戳 , temperature: 温度
case class SensorReading(id:String,timestamp:Long,temperature:Double)
