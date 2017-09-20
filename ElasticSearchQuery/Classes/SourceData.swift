//
//  SourceData.swift
//  Pods
//
//  Created by Andre Tsuyoshi Sakiyama on 9/19/17.
//
//

import Foundation
import ObjectMapper

class SourceData: NSObject, Mappable {
    var datetime: String?
    var datetime_idx: Int?
    var dust: Int?
    var humidity: Int?
    var temperature: Int?
    var methane: Int?
    var co: Int?
    
    override init() {
        super.init()
    }
    
    convenience required init?(map: Map){
        self.init()
    }
    
    func mapping(map: Map) {
        datetime <- map["datetime"]
        datetime_idx <- map["datetime_idx"]
        dust <- map["dust"]
        humidity <- map["humidity"]
        temperature <- map["temperature"]
        methane <- map["methane"]
        co <- map["co"]
    }
    
}
