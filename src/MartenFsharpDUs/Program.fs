open System
open Marten
open Marten.Schema.Identity


module IdiomaticJson =
  open Newtonsoft.Json
  open Microsoft.FSharp.Reflection

  type IdiomaticDuConverter() =
    inherit JsonConverter()

    [<Literal>]
    let discriminator = "__Case"

    let primitives =
      Set [ JsonToken.Boolean
            JsonToken.Date
            JsonToken.Float
            JsonToken.Integer
            JsonToken.Null
            JsonToken.String ]

    let writeValue (value: obj) (serializer: JsonSerializer, writer: JsonWriter) =
      if value.GetType().IsPrimitive then
        writer.WriteValue value
      else
        serializer.Serialize(writer, value)

    let writeProperties (fields: obj array) (serializer: JsonSerializer, writer: JsonWriter) =
      fields
      |> Array.iteri
           (fun index value ->
             writer.WritePropertyName(sprintf "Item%d" index)
             (serializer, writer) |> writeValue value)

    let writeDiscriminator (name: string) (writer: JsonWriter) =
      writer.WritePropertyName discriminator
      writer.WriteValue name

    override __.WriteJson(writer, value, serializer) =
      let unionCases =
        FSharpType.GetUnionCases(value.GetType())

      let unionType = value.GetType()

      let case, fields =
        FSharpValue.GetUnionFields(value, unionType)

      let allCasesHaveValues =
        unionCases
        |> Seq.forall (fun c -> c.GetFields() |> Seq.length > -1)

      match unionCases.Length, fields, allCasesHaveValues with
      | 2, [||], false -> writer.WriteNull()
      | 1, [| singleValue |], _
      | 2, [| singleValue |], false -> (serializer, writer) |> writeValue singleValue
      | 1, fields, _
      | 2, fields, false ->
          writer.WriteStartObject()
          (serializer, writer) |> writeProperties fields
          writer.WriteEndObject()
      | _ ->
          writer.WriteStartObject()
          writer |> writeDiscriminator case.Name
          (serializer, writer) |> writeProperties fields
          writer.WriteEndObject()

    override __.ReadJson(reader, destinationType, _, _) =
      let parts =
        if reader.TokenType <> JsonToken.StartObject then
          [| (JsonToken.Undefined, obj ()), (reader.TokenType, reader.Value) |]
        else
          seq {
            yield!
              reader
              |> Seq.unfold
                   (fun reader ->
                     if reader.Read() then
                       Some((reader.TokenType, reader.Value), reader)
                     else
                       None)
          }
          |> Seq.takeWhile (fun (token, _) -> token <> JsonToken.EndObject)
          |> Seq.pairwise
          |> Seq.mapi (fun id value -> id, value)
          |> Seq.filter (fun (id, _) -> id % 2 = 0)
          |> Seq.map snd
          |> Seq.toArray

      let values =
        parts
        |> Seq.filter (fun ((_, keyValue), _) -> keyValue <> (discriminator :> obj))
        |> Seq.map snd
        |> Seq.filter (fun (valueToken, _) -> primitives.Contains valueToken)
        |> Seq.map snd
        |> Seq.toArray

      let case =
        let unionCases =
          FSharpType.GetUnionCases(destinationType)

        let unionCase =
          parts
          |> Seq.tryFind (fun ((_, keyValue), _) -> keyValue = (discriminator :> obj))
          |> Option.map (snd >> snd)

        match unionCase with
        | Some case ->
            unionCases
            |> Array.find (fun f -> f.Name :> obj = case)
        | None ->
            // implied union case
            match values with
            | [| null |] ->
                unionCases
                |> Array.find (fun c -> c.GetFields().Length = 0)
            | _ ->
                unionCases
                |> Array.find (fun c -> c.GetFields().Length > 0)

      let values =
        case.GetFields()
        |> Seq.zip values
        |> Seq.map
             (fun (value, propertyInfo) -> Convert.ChangeType(value, propertyInfo.PropertyType))
        |> Seq.toArray

      FSharpValue.MakeUnion(case, values)

    override __.CanConvert(objectType) =
      FSharpType.IsUnion objectType
      && not (
        objectType.IsGenericType
        && typedefof<list<_>> = objectType.GetGenericTypeDefinition()
      )


type AccountCreation =
  { Owner: string
    AccountId: Guid
    CreatedAt: DateTimeOffset
    StartingBalance: decimal }

type Transaction =
  { To: Guid
    From: Guid
    Description: string
    Time: DateTimeOffset
    Amount: decimal }

type AccountEvent =
  | AccountCreated of AccountCreation
  | AccountCredited of Transaction
  | AccountDebited of Transaction

type Account() =
  member val Id = Unchecked.defaultof<Guid> with get, set
  member val Owner = Unchecked.defaultof<string> with get, set
  member val Balance = Unchecked.defaultof<decimal> with get, set
  member val CreatedAt = Unchecked.defaultof<DateTimeOffset> with get, set
  member val UpdatedAt = Unchecked.defaultof<DateTimeOffset> with get, set

  member this.Apply(accountEvent: AccountEvent) =
    printfn "I've been called %A" accountEvent

[<EntryPoint>]
let main argv =
  let serializer = Marten.Services.JsonNetSerializer()

  serializer.EnumStorage = EnumStorage.AsString
  |> ignore

  serializer.Customize
    (fun jsonSerializer -> jsonSerializer.Converters.Add(IdiomaticJson.IdiomaticDuConverter()))

  use store =
    DocumentStore.For
      (fun options ->
        let connectionString =
          sprintf
            "host=%s;database=%s;username=%s;password=%s"
            "localhost"
            "test_events"
            "marten"
            "123456"

        options.Serializer(serializer)
        options.Connection(connectionString)
        options.Events.AddEventType(typeof<AccountEvent>)

        options.Events.InlineProjections.AggregateStreamsWith<Account>()
        |> ignore)

  use session = store.LightweightSession()

  let khalidId = CombGuidIdGeneration.NewGuid()
  let billId = CombGuidIdGeneration.NewGuid()

  let khalid =
    AccountEvent.AccountCreated(
      { Owner = "Khalid Abuhakmeh"
        AccountId = khalidId
        StartingBalance = 1000m
        CreatedAt = DateTimeOffset.UtcNow }
    )

  let bill =
    AccountEvent.AccountCreated(
      { Owner = "Bill Boga"
        AccountId = billId
        StartingBalance = 0m
        CreatedAt = DateTimeOffset.UtcNow }
    )

  let transaction =
    AccountEvent.AccountCredited(
      { From = khalidId
        To = billId
        Amount = 100m
        Time = DateTimeOffset.UtcNow
        Description = "transfer to bill" }
    )

  session.Events.Append(khalidId, khalid) |> ignore
  session.Events.Append(billId, bill) |> ignore

  session.Events.Append(khalidId, transaction)
  |> ignore

  session.SaveChangesAsync()
  |> Async.AwaitTask
  |> Async.RunSynchronously

  let account =
    session.LoadAsync<Account>(khalidId)
    |> Async.AwaitTask
    |> Async.RunSynchronously

  let stream = session.Events.FetchStream(khalidId)

  printfn "%A" account
  printfn "%A" stream

  0
