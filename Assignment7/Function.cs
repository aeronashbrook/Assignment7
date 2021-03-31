using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon.Lambda.DynamoDBEvents;
using Amazon.Lambda.Core;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.Lambda.APIGatewayEvents;
using Newtonsoft.Json;
using Amazon.DynamoDBv2.Model;
using Amazon.DynamoDBv2;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace Assignment7
{

    public class Item
    {
        public string itemId;
        public string company;
        public string description;
        public double rating;
        public string type;
        public string lastInstanceOfWord;
    }
    public class typeRating
    {
        public string types;
        public string counts;
        public string avgRating;
    }
    public class Function
    {
        private static AmazonDynamoDBClient client = new AmazonDynamoDBClient();

        public async Task<List<Item>> FunctionHandler(DynamoDBEvent input, ILambdaContext context)
        {
            Table table = Table.LoadTable(client, "RatingsByTypes");
            List<Item> items = new List<Item>();
            List<DynamoDBEvent.DynamodbStreamRecord> records = (List<DynamoDBEvent.DynamodbStreamRecord>)input.Records;
            if (records.Count > 0)
            {
                DynamoDBEvent.DynamodbStreamRecord record = records[0];
                if (record.EventName.Equals("INSERT"))
                {
                    Document myDoc = Document.FromAttributeMap(record.Dynamodb.NewImage); //.Keys
                    Item myItem = JsonConvert.DeserializeObject<Item>(myDoc.ToJson());


                    var request1 = new GetItemRequest
                    {
                        TableName = "RatingsByTypes",
                        Key = new Dictionary<string, AttributeValue>()
                        {
                            { "types", new AttributeValue { S = myItem.type } }
                        },
                        ProjectionExpression = "types, counts, avgRating",
                        ConsistentRead = true
                    };
                    var response1 = await client.GetItemAsync(request1);
                    Document doc = Document.FromAttributeMap(response1.Item);
                    typeRating myType = JsonConvert.DeserializeObject<typeRating>(doc.ToJson());

                    //var attributeList = response1.Item;

                    double newRating = 0;
                    if (myItem.company == "B")
                    {
                        newRating = myItem.rating / 2;
                    } else
                    {
                        newRating = myItem.rating;
                    }

                    double myRating = 0;
                    if (myType.avgRating != null) {
                        myRating = Double.Parse(myType.avgRating);
                    }
                    double myCounts = 0;
                    if (myType.avgRating != null)
                    {
                        myCounts = Double.Parse(myType.counts);
                    }

                    double averageRating = ((myRating * myCounts) + newRating) / (myCounts + 1);

                    Console.WriteLine("type: " + myType.types);
                    Console.WriteLine("count: " + myType.counts);
                    Console.WriteLine("avgRating: " + myType.avgRating);
                    Console.WriteLine("new avgRating: " + averageRating.ToString());

                    var request = new UpdateItemRequest
                    {
                        TableName = "RatingsByTypes",
                        Key = new Dictionary<string, AttributeValue>
                        {
                            {"types", new AttributeValue {S = myItem.type} }
                        },
                        AttributeUpdates = new Dictionary<string, AttributeValueUpdate>()
                        {
                            {
                                "counts",
                                new AttributeValueUpdate {Action = "ADD", Value = new AttributeValue { N = "1"}}
                            },
                                                        {
                                "avgRating",
                                new AttributeValueUpdate {Action = "PUT", Value = new AttributeValue { N = Math.Round(averageRating, 2).ToString()}}
                            },
                        },
                    };                                
                    await client.UpdateItemAsync(request);
                }
            }
            return items;
        }

    }
}
