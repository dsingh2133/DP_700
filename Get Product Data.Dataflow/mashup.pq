[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "fdcb52e6-6235-412a-9255-f9ea8d995e43"]}[Data]{[lakehouseId = "4a0b3e5c-5c4d-49ad-872f-d401db354020"]}[Data];
[BindToDefaultDestination = true]
shared products = let
  Source = Csv.Document(Web.Contents("https://raw.githubusercontent.com/MicrosoftLearning/dp-data/main/products.csv"), [Delimiter = ",", Columns = 4, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"ProductID", Int64.Type}, {"ProductName", type text}, {"Category", type text}, {"ListPrice", type number}})
in
  #"Changed column type";
