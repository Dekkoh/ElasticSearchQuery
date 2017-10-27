import Foundation
import Alamofire
import ObjectMapper

public class ElasticSearchQuery {
    static private var urlString : String = ""
    static private var pageSize = 10000.0
    static private var data: [[ElasticSearchData]] = []
    
    static private func buildBody(sortingFeature: String,
                                  orderType: String,
                                  field: String,
                                  startTimestamp: String,
                                  endTimestamp: String) -> [String : Any] {
        let startPosition = 0
        
        let body : [String : Any] = [
            "sort": [
                [sortingFeature : ["order": orderType]]
            ],
            "size": pageSize,
            "from": startPosition,
            "query": [
                "filtered": [
                    "filter": [
                        "bool": [
                            "must": [[
                                "exists": [
                                    "field": field
                                ]
                                ],
                                     [
                                        "range": [
                                            "datetime_idx": [
                                                "gte": startTimestamp,
                                                "lte": endTimestamp
                                            ]
                                        ]
                                ]]
                        ]
                    ]
                ]
            ]
        ]
        
        return body
    }
    
    static private func buildRequest(body: Data) -> URLRequest {
        var request = URLRequest(url: URL( string: urlString)!)
        request.httpMethod = HTTPMethod.post.rawValue
        request.setValue("application/json; charset=UTF-8", forHTTPHeaderField: "Content-Type")
        request.httpBody = body
        
        return request
        
    }
    
    
    static private func dictToJSON(data: [String : Any]) -> Data {
        let bodyJSON =   try! JSONSerialization.data(withJSONObject: data, options: JSONSerialization.WritingOptions.prettyPrinted)
        
        let json = NSString(data: bodyJSON, encoding: String.Encoding.utf8.rawValue)
        
        let jsonData = json!.data(using: String.Encoding.utf8.rawValue);
        
        return jsonData!
    }
    
    
    static private func getDataSize(body: [String  : Any],
                                    callback: @escaping (Int, Int, Int, [Int], [Int], Int) -> Void,
                                    field: String,
                                    sortingFeature: String,
                                    firstDataTimestamp: String) {
        
        var modifiedBody = body
        modifiedBody["size"] = 0
        let jsonBody = dictToJSON(data: modifiedBody)
        
        let request = buildRequest(body: jsonBody)
        
        Alamofire.request(request).responseJSON { (response) in
            var totalPages: Int = 0
            
            switch response.result {
            case .success:
                guard let result = response.result.value as? [String : Any] else {
                    print("Data Size Parsing Error")
                    return
                }
                guard let hits = result["hits"] as? [String : Any] else {
                    print("Data Size Parsing Error")
                    return
                }
                
                let total = hits["total"] as! Double
                
                if (total == 0) {
                    totalPages = 0
                } else {
                    totalPages = Int(ceil(total / self.pageSize))
                }
                
                break
            case .failure(let error):
                print("Get Data Size with error: \(error)")
                break
            }
            
            makeAssynchronousRequest(body: body, pages: totalPages, callback: callback, field: field, sortingFeature: sortingFeature, firstDataTimestamp: firstDataTimestamp)
            
        }
    }
    
    static private func makeAssynchronousRequest(body: [String : Any],
                                                 pages: Int,
                                                 callback: @escaping (Int, Int, Int, [Int], [Int], Int) -> Void,
                                                 field: String,
                                                 sortingFeature: String,
                                                 firstDataTimestamp: String) {
        
        self.data = []
        let dispatchGroup = DispatchGroup()
        
        for i in (0..<pages) {
            dispatchGroup.enter()
            
            var modifiedBody = body
            modifiedBody["from"] = Int(pageSize) * (i)
            let jsonBody = dictToJSON(data: modifiedBody)
            
            let request = buildRequest(body: jsonBody)
            
            Alamofire.request(request).responseJSON { (response) in
                switch response.result {
                case .success:
                    guard let result = response.result.value as? [String : Any] else {
                        print("Data Parsing Error")
                        return
                    }
                    guard let hits = result["hits"] as? [String : Any] else {
                        print("Data Parsing Error")
                        return
                    }
                    guard let source = hits["hits"] as? Array<[String : Any]> else {
                        print("Data Parsing Error")
                        return
                    }
                    
                    self.data = Array<[ElasticSearchData]>()
                    self.data.append(Mapper<ElasticSearchData>().mapArray(JSONArray: source) as [ElasticSearchData]!)
                    dispatchGroup.leave()
                    
                    break
                    
                case .failure(let error):
                    print("Request failed with error: \(error)")
                    dispatchGroup.leave()
                    return
                }
            }
        }
        
        DispatchQueue.global(qos: .background).async {
            dispatchGroup.wait()
            DispatchQueue.main.async {
                self.data.sort(by: { (a, b) -> Bool in
                    if ((a[0].sourceData?.datetime_idx)! < (b[0].sourceData?.datetime_idx)!){
                        return true
                    }
                    return false
                })
                
                let reducedData = self.data.reduce([], +)
                getFirstDataBeforeStart(rawData: reducedData, callback: callback, field: field, sortingFeature: sortingFeature, firstDataTimestamp: firstDataTimestamp)
                
            }
        }
    }
    
    static private func getFirstDataBeforeStart(rawData: [ElasticSearchData],
                                                callback: @escaping (Int, Int, Int, [Int], [Int], Int) -> Void,
                                                field: String,
                                                sortingFeature: String,
                                                firstDataTimestamp: String) {
        let orderType: String = "desc"
        let startTimestamp: String = "0"
        var modifiedBody = buildBody(sortingFeature: sortingFeature, orderType: orderType, field: field, startTimestamp: startTimestamp, endTimestamp: firstDataTimestamp)
        modifiedBody["size"] = 1
        
        let jsonBody = dictToJSON(data: modifiedBody)
        let request = buildRequest(body: jsonBody)
        
        Alamofire.request(request).responseJSON { (response) in
            var firstData = ElasticSearchData()
            
            switch response.result {
            case .success:
                guard let result = response.result.value as? [String : Any] else {
                    print("First Data Parsing Error")
                    return
                }
                guard let hits = result["hits"] as? [String : Any] else {
                    print("First Data Parsing Error")
                    return
                }
                guard let source = hits["hits"] as? Array<[String : Any]> else {
                    print("First Data Parsing Error")
                    return
                }
                firstData = Mapper<ElasticSearchData>().mapArray(JSONArray: source)[0]
                //                self.data = Array<[ElasticSearchData]>()
                //                self.data.append(Mapper<ElasticSearchData>().mapArray(JSONArray: source) as [ElasticSearchData]!)
                
                break
                
            case .failure(let error):
                print("Request failed with error: \(error)")
                return
            }
            
            extractData(rawData: rawData, firstData: firstData, callback: callback, field: field, sortingFeature: sortingFeature, firstDataTimestamp: firstDataTimestamp)
            
        }
        
        
    }
    
    static private func extractData(rawData: [ElasticSearchData],
                                    firstData: ElasticSearchData,
                                    callback: @escaping (Int, Int, Int, [Int], [Int], Int) -> Void,
                                    field: String,
                                    sortingFeature: String,
                                    firstDataTimestamp: String) {
        var average = 0
        var maximum = Int.min
        var minimum = Int.max
        var dataArray: [Int] = []
        var timestampArray: [Int] = []
        var elemData = 0
        var extractedFirstData = 0
        
        for elem in rawData {
            
            guard elem.sourceData != nil else {
                break
            }
            
            switch field {
            case "dust":
                guard elem.sourceData?.dust != nil else {
                    break
                }
                elemData = (elem.sourceData?.dust!)!
                break
            case "temperature":
                guard elem.sourceData?.temperature != nil else {
                    break
                }
                elemData = (elem.sourceData?.temperature!)!
                break
            case "humidity":
                guard elem.sourceData?.humidity != nil else {
                    break
                }
                elemData = (elem.sourceData?.humidity!)!
                break
            case "methane":
                guard elem.sourceData?.methane != nil else {
                    break
                }
                elemData = (elem.sourceData?.methane!)!
                break
            case "co":
                guard elem.sourceData?.co != nil else {
                    break
                }
                elemData = (elem.sourceData?.co!)!
                break
            default:
                break
            }
            
            average += elemData
            
            if (maximum < elemData) {
                maximum = elemData
            }
            
            if (minimum > elemData) {
                minimum = elemData
            }
            
            dataArray.append(elemData)
            timestampArray.append((elem.sourceData?.datetime_idx)!)
            
        }
        
        if (rawData.count == 0) {
            average = 0
            maximum = 0
            minimum = 0
        } else {
            average /= rawData.count
        }
        
        switch field {
        case "dust":
            guard firstData.sourceData?.dust != nil else {
                break
            }
            extractedFirstData = (firstData.sourceData?.dust)!
            break
        case "temperature":
            guard firstData.sourceData?.temperature != nil else {
                break
            }
            extractedFirstData = (firstData.sourceData?.temperature)!
            break
        case "humidity":
            guard firstData.sourceData?.humidity != nil else {
                break
            }
            extractedFirstData = (firstData.sourceData?.humidity)!
            break
        case "methane":
            guard firstData.sourceData?.methane != nil else {
                break
            }
            extractedFirstData = (firstData.sourceData?.methane)!
            break
        case "co":
            guard firstData.sourceData?.co != nil else {
                break
            }
            extractedFirstData = (firstData.sourceData?.co)!
            break
        default:
            break
        }
        
        callback(average, maximum, minimum, dataArray, timestampArray, extractedFirstData)
    }
    
    static public func queryData(field: String = "dust", sortingFeature: String = "datetime_idx", orderType: String = "asc", startTimestamp: String = "1491004800000", endTimestamp: String = "1499177600000", callback: @escaping (Int, Int, Int, [Int], [Int], Int) -> Void) {
        
        let firstDataTimestamp = String(Int(startTimestamp)! - 1)
        
        let body = buildBody(sortingFeature: sortingFeature, orderType: orderType, field: field, startTimestamp: startTimestamp, endTimestamp: endTimestamp)
        getDataSize(body: body, callback: callback, field: field, sortingFeature: sortingFeature, firstDataTimestamp: firstDataTimestamp)
    }
    
    static public func getDust(from: String, to: String, callback: @escaping (Int, Int, Int, [Int], [Int], Int) -> Void) {
        let field = "dust"
        let sortingFeature = "datetime_idx"
        let orderType = "asc"
        
        queryData(field: field, sortingFeature: sortingFeature, orderType: orderType, startTimestamp: from, endTimestamp: to, callback: callback)
    }
    
    static public func getHumidity(from: String, to: String, callback: @escaping (Int, Int, Int, [Int], [Int], Int) -> Void) {
        let field = "humidity"
        let sortingFeature = "datetime_idx"
        let orderType = "asc"
        
        queryData(field: field, sortingFeature: sortingFeature, orderType: orderType, startTimestamp: from, endTimestamp: to, callback: callback)
    }
    
    static public func getTemperature(from: String, to: String, callback: @escaping (Int, Int, Int, [Int], [Int], Int) -> Void) {
        let field = "temperature"
        let sortingFeature = "datetime_idx"
        let orderType = "asc"
        
        queryData(field: field, sortingFeature: sortingFeature, orderType: orderType, startTimestamp: from, endTimestamp: to, callback: callback)
    }
    
    static public func getMethane(from: String, to: String, callback: @escaping (Int, Int, Int, [Int], [Int], Int) -> Void) {
        let field = "methane"
        let sortingFeature = "datetime_idx"
        let orderType = "asc"
        
        queryData(field: field, sortingFeature: sortingFeature, orderType: orderType, startTimestamp: from, endTimestamp: to, callback: callback)
    }
    
    static public func getCO(from: String, to: String, callback: @escaping (Int, Int, Int, [Int], [Int], Int) -> Void) {
        let field = "co"
        let sortingFeature = "datetime_idx"
        let orderType = "asc"
        
        queryData(field: field, sortingFeature: sortingFeature, orderType: orderType, startTimestamp: from, endTimestamp: to, callback: callback)
    }
    
    
    static public func setURL(url : String) -> Void {
        self.urlString = url
    }
}

