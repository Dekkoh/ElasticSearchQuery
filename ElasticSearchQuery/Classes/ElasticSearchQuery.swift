import Foundation
import Alamofire
import ObjectMapper

public class ElasticSearchQuery {
    private var urlString : String = ""
    private var pageSize = 10000.0
    private var data: [[ElasticSearchData]] = []
    private var callback: (Int, Int, Int, [Int], [Int]) -> Void = {_ in }
    
    private func buildBody(field: String!, sortingFeature: String!, orderType: String!, startTimestamp: String!, endTimestamp: String!) -> [String : Any] {
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
    
    private func buildRequest(body: Data) -> URLRequest {
        var request = URLRequest(url: URL( string: urlString)!)
        request.httpMethod = HTTPMethod.post.rawValue
        request.setValue("application/json; charset=UTF-8", forHTTPHeaderField: "Content-Type")
        request.httpBody = body
        
        return request
        
    }

    
    private func dictToJSON(data: [String : Any]) -> Data {
        let bodyJSON =   try! JSONSerialization.data(withJSONObject: data, options: JSONSerialization.WritingOptions.prettyPrinted)
        
        let json = NSString(data: bodyJSON, encoding: String.Encoding.utf8.rawValue)
        if let json = json {
            print(json)
        }
        
        let jsonData = json!.data(using: String.Encoding.utf8.rawValue);
        
        return jsonData!
    }
    
    private func extractData(rawData: [ElasticSearchData], field: String) {
        var average = 0
        var maximum = Int.min
        var minimum = Int.max
        var dataArray: [Int] = []
        var timestampArray: [Int] = []
        var elemData = 0
        
        for elem in rawData {
            switch field {
            case "dust":
                elemData = (elem.sourceData?.dust!)!
                break
            case "humidity":
                elemData = (elem.sourceData?.humidity!)!
                break
            case "methane":
                elemData = (elem.sourceData?.methane!)!
                break
            case "co":
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
        
        average /= rawData.count
        
        callback(average, maximum, minimum, dataArray, timestampArray)
    }
    
    
    private func getDataSize(body: [String  : Any], field: String, completion:@escaping ([String : Any], String, Int) -> Void) {
        var modifiedBody = body
        modifiedBody["size"] = 0
        let jsonBody = dictToJSON(data: modifiedBody)
        
        let request = buildRequest(body: jsonBody)
        
        Alamofire.request(request).responseJSON { (response) in
            var totalPages: Int = 0
            
            switch response.result {
            case .success:
                guard let result = response.result.value as? [String : Any] else {
                    return
                }
                guard let hits = result["hits"] as? [String : Any] else {
                    return
                }
                
                let total = hits["total"] as! Double
                
                totalPages = Int(ceil(total / self.pageSize))
                
                break
            case .failure(let error):
                print(error)
                break
            }
            
            completion(body, field, totalPages)
            
        }
    }
    
    private func makeAssynchronousRequest(body: [String : Any], field: String, pages: Int) {
        
        let dispatchGroup = DispatchGroup()
        
        for _ in (0..<pages) {
            dispatchGroup.enter()
        }
        
        for var i in (0..<pages) {
            var modifiedBody = body
            modifiedBody["from"] = Int(pageSize) * (i + 1)
            let jsonBody = dictToJSON(data: modifiedBody)
            
            let request = buildRequest(body: jsonBody)
            
            Alamofire.request(request).responseJSON { (response) in
                switch response.result {
                case .success:
                    guard let result = response.result.value as? [String : Any] else {
                        return
                    }
                    guard let hits = result["hits"] as? [String : Any] else {
                        return
                    }
                    guard let source = hits["hits"] as? Array<[String : Any]> else {
                        return
                    }
                    
                    self.data = Array<[ElasticSearchData]>()
                    self.data.append(Mapper<ElasticSearchData>().mapArray(JSONArray: source) as [ElasticSearchData]!)
                    dispatchGroup.leave()
                    
                    break
                    
                case .failure(let error):
                    print("Request failed with error: \(error)")
                    //callback(response.result.value as? NSMutableDictionary,error as NSError?)
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
                print(reducedData)
                self.extractData(rawData: reducedData, field: field)

            }
        }
    }
    
    public func queryData(field: String = "dust", sortingFeature: String = "datetime_idx", orderType: String = "asc", startTimestamp: String = "1491004800000", endTimestamp: String = "1499177600000", callback: @escaping (Int, Int, Int, [Int], [Int]) -> Void) {
        self.callback = callback
        let body = buildBody(field: field, sortingFeature: sortingFeature, orderType: orderType, startTimestamp: startTimestamp, endTimestamp: endTimestamp)
        getDataSize(body: body, field: field, completion: makeAssynchronousRequest)
    }
    
    public func setURL(url : String) {
        self.urlString = url
    }
}
