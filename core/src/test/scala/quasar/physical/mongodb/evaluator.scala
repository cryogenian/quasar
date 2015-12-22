package quasar.physical.mongodb

import quasar.Predef._
import quasar.fs.Path
import quasar.javascript._
import quasar.physical.mongodb.accumulator._
import quasar.physical.mongodb.expression._

import scala.collection.immutable.ListMap

import org.specs2.mutable._
import org.specs2.scalaz._
import scalaz._, Scalaz._

class EvaluatorSpec extends Specification with DisjunctionMatchers {
  "evaluate" should {
    import Workflow._

    "write trivial workflow to JS" in {
      val wf = $read(Collection("db", "zips"))

      MongoDbEvaluator.toJS(crystallize(wf)) must beRightDisjunction(
        s"""db.zips.find();
           |""".stripMargin)
    }

    "write trivial workflow to JS with fancy collection name" in {
      val wf = $read(Collection("db", "tmp.123"))

      MongoDbEvaluator.toJS(crystallize(wf)) must beRightDisjunction(
        s"""db.getCollection(\"tmp.123\").find();
           |""".stripMargin)
    }

    "write workflow with simple pure value" in {
      val wf = $pure(Bson.Doc(ListMap("foo" -> Bson.Text("bar"))))

        MongoDbEvaluator.toJS(crystallize(wf)) must beRightDisjunction(
          """[{ "foo": "bar" }];
            |""".stripMargin)
    }

    "write workflow with multiple pure values" in {
      val wf = $pure(Bson.Arr(List(
        Bson.Doc(ListMap("foo" -> Bson.Int64(1))),
        Bson.Doc(ListMap("bar" -> Bson.Int64(2))))))

        MongoDbEvaluator.toJS(crystallize(wf)) must beRightDisjunction(
          """[{ "foo": NumberLong(1) }, { "bar": NumberLong(2) }];
            |""".stripMargin)
    }

    "fail with non-doc pure value" in {
      val wf = $pure(Bson.Text("foo"))

      MongoDbEvaluator.toJS(crystallize(wf)) must beRightDisjunction(
        """["foo"];
          |""".stripMargin)
    }

    "fail with multiple pure values, one not a doc" in {
      val wf = $pure(Bson.Arr(List(
        Bson.Doc(ListMap("foo" -> Bson.Int64(1))),
        Bson.Int64(2))))

        MongoDbEvaluator.toJS(crystallize(wf)) must beRightDisjunction(
          """[{ "foo": NumberLong(1) }, NumberLong(2)];
            |""".stripMargin)
    }

    "write simple query to JS" in {
      val wf = chain(
        $read(Collection("db", "zips")),
        $match(Selector.Doc(
          BsonField.Name("pop") -> Selector.Gte(Bson.Int64(1000)))))

      MongoDbEvaluator.toJS(crystallize(wf)) must beRightDisjunction(
        """db.zips.find({ "pop": { "$gte": NumberLong(1000) } });
          |""".stripMargin)
    }

    "write limit to JS" in {
      val wf = chain(
        $read(Collection("db", "zips")),
        $limit(10))

      MongoDbEvaluator.toJS(crystallize(wf)) must beRightDisjunction(
        """db.zips.find().limit(10);
          |""".stripMargin)
    }

    "write project and limit to JS" in {
      val wf = chain(
        $read(Collection("db", "zips")),
        $limit(10),
        $project(
          Reshape(ListMap(
            BsonField.Name("city") -> $include().right)),
          IdHandling.ExcludeId))

      MongoDbEvaluator.toJS(crystallize(wf)) must beRightDisjunction(
        """db.zips.find({ "city": true, "_id": false }).limit(10);
          |""".stripMargin)
    }

    "write filter, project, and limit to JS" in {
      val wf = chain(
        $read(Collection("db", "zips")),
        $match(Selector.Doc(
          BsonField.Name("pop") -> Selector.Lt(Bson.Int64(1000)))),
        $limit(10),
        $project(
          Reshape(ListMap(
            BsonField.Name("city") -> $include().right)),
          IdHandling.ExcludeId))

      MongoDbEvaluator.toJS(crystallize(wf)) must beRightDisjunction(
        """db.zips.find(
          |  { "pop": { "$lt": NumberLong(1000) } },
          |  { "city": true, "_id": false }).limit(
          |  10);
          |""".stripMargin)

    }

    "write simple count to JS" in {
      val wf = chain(
        $read(Collection("db", "zips")),
        $match(Selector.Doc(
          BsonField.Name("pop") -> Selector.Gte(Bson.Int64(1000)))),
        $group(
          Grouped(ListMap(
            BsonField.Name("num") -> $sum($literal(Bson.Int32(1))))),
          $literal(Bson.Null).right))

      MongoDbEvaluator.toJS(crystallize(wf)) must beRightDisjunction(
        """[{ "num": db.zips.count({ "pop": { "$gte": NumberLong(1000) } }) }];
          |""".stripMargin)
    }

    "write simple pipeline workflow to JS" in {
      val wf = chain(
        $read(Collection("db", "zips")),
        $project(Reshape(ListMap(
          BsonField.Name("popInK") -> $divide($field("pop"), $literal(Bson.Int64(1000))).right)),
          IdHandling.ExcludeId))

      MongoDbEvaluator.toJS(crystallize(wf)) must beRightDisjunction(
        """db.zips.aggregate(
          |  [
          |    {
          |      "$project": {
          |        "popInK": { "$divide": ["$pop", { "$literal": NumberLong(1000) }] },
          |        "_id": false
          |      }
          |    }],
          |  { "allowDiskUse": true });
          |""".stripMargin)
    }

    "write chained pipeline workflow to JS find()" in {
      val wf = chain(
        $read(Collection("db", "zips")),
        $match(Selector.Doc(
          BsonField.Name("pop") -> Selector.Lte(Bson.Int64(1000)))),
        $match(Selector.Doc(
          BsonField.Name("pop") -> Selector.Gte(Bson.Int64(100)))),
        $sort(NonEmptyList(BsonField.Name("city") -> Ascending)))

      MongoDbEvaluator.toJS(crystallize(wf)) must beRightDisjunction(
        """db.zips.find(
          |  {
          |    "$and": [
          |      { "pop": { "$lte": NumberLong(1000) } },
          |      { "pop": { "$gte": NumberLong(100) } }]
          |  }).sort(
          |  { "city": NumberInt(1) });
          |""".stripMargin)
    }

    "write chained pipeline workflow to JS" in {
      val wf = chain(
        $read(Collection("db", "zips")),
        $match(Selector.Doc(
          BsonField.Name("pop") -> Selector.Lte(Bson.Int64(1000)))),
        $match(Selector.Doc(
          BsonField.Name("pop") -> Selector.Gte(Bson.Int64(100)))),
        $group(
          Grouped(ListMap(
            BsonField.Name("pop") -> $sum($field("pop")))),
          $field("city").right),
        $sort(NonEmptyList(BsonField.Name("_id") -> Ascending)))

      MongoDbEvaluator.toJS(crystallize(wf)) must beRightDisjunction(
        """db.zips.aggregate(
          |  [
          |    {
          |      "$match": {
          |        "$and": [
          |          { "pop": { "$lte": NumberLong(1000) } },
          |          { "pop": { "$gte": NumberLong(100) } }]
          |      }
          |    },
          |    { "$group": { "pop": { "$sum": "$pop" }, "_id": "$city" } },
          |    { "$sort": { "_id": NumberInt(1) } }],
          |  { "allowDiskUse": true });
          |""".stripMargin)
    }

    "write map-reduce Workflow to JS" in {
      val wf = chain(
        $read(Collection("db", "zips")),
        $map($Map.mapKeyVal(("key", "value"),
          Js.Select(Js.Ident("value"), "city"),
          Js.Select(Js.Ident("value"), "pop")),
          ListMap()),
        $reduce(Js.AnonFunDecl(List("key", "values"), List(
          Js.Return(Js.Call(
            Js.Select(Js.Ident("Array"), "sum"),
            List(Js.Ident("values")))))),
          ListMap()))

      MongoDbEvaluator.toJS(crystallize(wf)) must beRightDisjunction(
        """db.zips.mapReduce(
          |  function () {
          |    emit.apply(
          |      null,
          |      (function (key, value) { return [value.city, value.pop] })(
          |        this._id,
          |        this))
          |  },
          |  function (key, values) { return Array.sum(values) },
          |  { "out": { "inline": NumberLong(1) } });
          |""".stripMargin)
    }

    "write $where condition to JS" in {
      val wf = chain(
        $read(Collection("db", "zips2")),
        $match(Selector.Where(Js.Ident("foo"))))

      MongoDbEvaluator.toJS(crystallize(wf)) must beRightDisjunction(
        """db.zips2.mapReduce(
          |  function () {
          |    emit.apply(
          |      null,
          |      (function (key, value) { return [key, value] })(this._id, this))
          |  },
          |  function (key, values) { return values[0] },
          |  {
          |    "out": { "inline": NumberLong(1) },
          |    "query": { "$where": function () { return foo } }
          |  });
          |""".stripMargin)
    }

    "write join Workflow to JS" in {
      val wf =
        $foldLeft(
          chain(
            $read(Collection("db", "zips1")),
            $match(Selector.Doc(
              BsonField.Name("city") -> Selector.Eq(Bson.Text("BOULDER"))))),
          chain(
            $read(Collection("db", "zips2")),
            $match(Selector.Doc(
              BsonField.Name("pop") -> Selector.Lte(Bson.Int64(1000)))),
            $map($Map.mapKeyVal(("key", "value"),
              Js.Select(Js.Ident("value"), "city"),
              Js.Select(Js.Ident("value"), "pop")),
              ListMap()),
            $reduce(Js.AnonFunDecl(List("key", "values"), List(
              Js.Return(Js.Call(
                Js.Select(Js.Ident("Array"), "sum"),
                List(Js.Ident("values")))))),
              ListMap())))

      MongoDbEvaluator.toJS(crystallize(wf)) must beRightDisjunction(
        """db.zips1.aggregate(
          |  [
          |    { "$match": { "city": "BOULDER" } },
          |    { "$project": { "value": "$$ROOT" } },
          |    { "$out": "tmp.gen_0" }],
          |  { "allowDiskUse": true });
          |db.zips2.mapReduce(
          |  function () {
          |    emit.apply(
          |      null,
          |      (function (key, value) { return [value.city, value.pop] })(
          |        this._id,
          |        this))
          |  },
          |  function (key, values) { return Array.sum(values) },
          |  {
          |    "out": { "reduce": "tmp.gen_0", "db": "db", "nonAtomic": true },
          |    "query": { "pop": { "$lte": NumberLong(1000) } }
          |  });
          |db.tmp.gen_0.find();
          |""".stripMargin)
    }
  }

  "SimpleCollectionNamePattern" should {
    import JSExecutor._

    "match identifier" in {
      SimpleCollectionNamePattern.unapplySeq("foo") must beSome
    }

    "not match leading _" in {
      SimpleCollectionNamePattern.unapplySeq("_foo") must beNone
    }

    "match dot-separated identifiers" in {
      SimpleCollectionNamePattern.unapplySeq("foo.bar") must beSome
    }

    "match everything allowed" in {
      SimpleCollectionNamePattern.unapplySeq("foo2.BAR_BAZ") must beSome
    }

    "not match leading digit" in {
      SimpleCollectionNamePattern.unapplySeq("123") must beNone
    }

    "not match leading digit in second position" in {
      SimpleCollectionNamePattern.unapplySeq("foo.123") must beNone
    }
  }
}
