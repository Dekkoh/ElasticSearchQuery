//
//  ElasticSearchData.swift
//  Pods
//
//  Created by Andre Tsuyoshi Sakiyama on 9/19/17.
//
//

import Foundation
import ObjectMapper

class ElasticSearchData: NSObject, Mappable {
    var sourceData: SourceData?
    
    override init() {
        super.init()
    }
    
    convenience required init?(map: Map){
        self.init()
    }
    
    func mapping(map: Map) {
        sourceData <- map["_source"]
    }
    
}
