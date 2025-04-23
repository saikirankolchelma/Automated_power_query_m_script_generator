```m
let
    Source = Excel.Workbook(File.Contents("C:\\Users\\ksaik\\OneDrive\\Desktop\\MSCriptAutomation\\MSCriptAutomation\\combined_datasets.xlsx")),
    Clinical_Unit_Costs_Migrated_Da_Table = Source{[Item="Clinical Unit Costs_Migrated Da"]}[Data],
    #"Promoted Headers" = Table.PromoteHeaders(Clinical_Unit_Costs_Migrated_Da_Table, [PromoteAllScalars=true]),
    #"Changed Type" = Table.TransformColumnTypes(#"Promoted Headers",{{"Clinical Unit", type text}, {"Episode Start Datetime", type text}, {"MRN Number", type text}, {"Patient Address Zipcode", Int64.Type}, {"Daily Cost", type number}, {"Number of Records", Int64.Type}})
in
    #"Changed Type"
```