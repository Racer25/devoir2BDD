var MongoClient = require('mongodb').MongoClient,
	test = require('assert');

MongoClient.connect('mongodb://localhost:27017/dataBase', function(error, db) 
{
	if (error) return funcCallback(error);
	
    console.log("Connecté à la base de données");

	// Create a test collection
	var views=generateViews();
	var part2 = db.collection('part2');
	
	part2.removeMany(); //Efface tout (sync)
	
	part2.insertMany(views, {w:1}).then
	(
		function()
		{
			console.log("J'ai fini d'insérer");
			// Peform the map reduce
			console.log("Je vais faire un mapReduce");
			
			var map = function ()
			{
				key={url:this.url,hour:this.hour};
                
                var objectEmitted=new Object();
                Object.defineProperty(objectEmitted, this.user_id, {enumerable: true, 
                                                                    configurable: true, 
                                                                    writable: true, 
                                                                    value: 1});
				emit(key, objectEmitted);
			}

			var reduce = function (key, values)
			{
				var uniques=new Object();
				for(var i=0; i < values.length; i++)
				{
					var id=Object.keys(values[i])[0];
                    
					if(uniques.hasOwnProperty(id))
					{
                        uniques[id]+=values[i][id];
					}
					else
					{
						Object.defineProperty(uniques, id,  {enumerable: true, 
                                                             configurable: true, 
                                                             writable: true, 
                                                             value: values[i][id]});
					}
                    
				}
				return(uniques);
			}
			
			part2.mapReduce(map, reduce, {out: "part2Result"}).then
			(
				function()
			 {
				console.log("J'ai fini mon mapReduce");
				 db.close();
             }
			);
		}
	);
});

function randomIntInc (low, high) 
{
       return Math.floor(Math.random() * (high - low + 1) + low);
}

function generateViews()
{
	views = [];
	var urls = [
       "/",
       "/edmond",
       "/ksander",
       "/basile",
       "/ulrich",
       "/schaal",
       "/theo",
   ];


   //Generate views
   for (var i = 0; i < 100; i++) {


       var visite = {


           url :  "",  //random parmi liste
           user_id: "", //random
           hour: "",    //0-23


       };


       visite.url = urls[ randomIntInc(0,urls.length-1)];
       visite.user_id = randomIntInc(0, 10);
       visite.hour = randomIntInc(0,23);


       views.push(visite);
   }
	return views;
}
