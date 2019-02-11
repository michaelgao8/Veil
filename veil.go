//package Veil
package main

import ( 
    "fmt"
    "encoding/json"
    "io/ioutil"
    "os"
)

type VeilMapFmt struct {
    Deidentify []string `json:"deidentify"`
    Timeshift []string  `json:"timeshift"`
}

func VeilParseJSON(file string) map[string]interface{} {
    jsonFile, err := os.Open(file)
    if err != nil {
        fmt.Println(err)
    }
    defer jsonFile.Close()

    jsonValue, _ := ioutil.ReadAll(jsonFile)

    var x map[string]interface{}
    json.Unmarshal(jsonValue, &x)

    return x
}

func VeilParseMap(file string, out VeilMapFmt) VeilMapFmt {
    jsonFile, err := os.Open(file)
    if err != nil {
        fmt.Println(err)
    }
    defer jsonFile.Close()

    jsonValue, _ := ioutil.ReadAll(jsonFile)

    json.Unmarshal(jsonValue, &out)

    return out
}

func VeilAdd(a, b float64) float64 {
    c := a + b
    return c
}

func main() {

    var mp VeilMapFmt

    out := VeilParseMap("veilMap.json", mp)
    y := VeilParseJSON("veilFixture.json")
    
    fmt.Println(out.Deidentify)
    fmt.Println(y["name"])

}
