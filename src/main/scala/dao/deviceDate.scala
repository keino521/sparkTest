package dao

case class deviceDate(id:Int,device:String) {

}
case class DeviceAlert(dcId: String, deviceType:String, ip:String, deviceId:Long, temp:Long, c02_level: Long, lat: Double, lon: Double)