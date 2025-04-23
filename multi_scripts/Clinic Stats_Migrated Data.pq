```m
let
    Source = Excel.Workbook(File.Contents("C:\\Users\\ksaik\\OneDrive\\Desktop\\MSCriptAutomation\\MSCriptAutomation\\combined_datasets.xlsx"), null, true),
    Clinic_Stats_Migrated_Data_Table = Source{[Name="Clinic Stats_Migrated Data"]}[Data],
    #"Promoted Headers" = Table.PromoteHeaders(Clinic_Stats_Migrated_Data_Table, [PromoteAllScalars=true]),
    #"Changed Type" = Table.TransformColumnTypes(#"Promoted Headers",{{"Admit Source", type text}, {"Admit Type", type text}, {"Appt Start Time", type text}, {"Check In Time", type text}, {"Clinical Unit", type text}, {"Cohort", type text}, {"Diagnosis Primary", type text}, {"Discharge Datetime", type text}, {"Encounter Number", type text}, {"Encounter Status", type text}, {"Gender", type text}, {"Hospital LOS", type text}, {"Hospital Room Number", type text}, {"MRN Number", type text}, {"Patient Address Zipcode", Int64.Type}, {"Procedure (Icd9) Primary", type text}, {"Religion", type text}, {"Strategic Planning Line Code", type text}, {"Age", Int64.Type}, {"Care Score", Int64.Type}, {"Logistics Score", Int64.Type}, {"LOS", type number}, {"Number of Patients", Int64.Type}, {"Number of Records", Int64.Type}, {"Wait Time Days", type number}, {"Wait Time KPI", type text}, {"Wait Time Min", Int64.Type}})
in
    #"Changed Type"
```