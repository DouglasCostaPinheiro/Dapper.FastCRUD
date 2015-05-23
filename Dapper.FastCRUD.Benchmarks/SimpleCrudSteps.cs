﻿namespace Dapper.FastCrud.Benchmarks
{
    using System.Linq;
    using Dapper.FastCrud.Tests;
    using Dapper.FastCrud.Tests.Models;
    using NUnit.Framework;
    using TechTalk.SpecFlow;
    using SimpleCrud = global::Dapper.SimpleCRUD;

    [Binding]
    public class SimpleCrudSteps:EntityGenerationSteps
    {
        private DatabaseTestContext _testContext;

        public SimpleCrudSteps(DatabaseTestContext testContext)
        {
            _testContext = testContext;
        }

        [When(@"I insert (.*) benchmark entities using Simple Crud")]
        public void WhenIInsertSingleIntKeyEntitiesUsingSimpleCrud(int entitiesCount)
        {
            var dbConnection = _testContext.DatabaseConnection;

            for (var entityIndex = 1; entityIndex <= entitiesCount; entityIndex++)
            {
                var generatedEntity = this.GenerateSimpleBenchmarkEntity(entityIndex);

                generatedEntity.Id = SimpleCrud.Insert(dbConnection, generatedEntity).Value;

                Assert.Greater(generatedEntity.Id, 1); // the seed starts from 2 in the db to avoid confusion with the number of rows modified
                _testContext.InsertedEntities.Add(generatedEntity);
            }
        }

        [When(@"I select all the benchmark entities using Simple Crud")]
        public void WhenISelectAllTheSingleIntKeyEntitiesUsingSimpleCrud()
        {
            var dbConnection = _testContext.DatabaseConnection;
            _testContext.QueriedEntities.AddRange(SimpleCrud.GetList<SimpleBenchmarkEntity>(dbConnection));
        }

        [When(@"I select all the benchmark entities that I previously inserted using Simple Crud")]
        public void WhenISelectAllTheSingleIntKeyEntitiesThatIPreviouslyInsertedUsingSimpleCrud()
        {
            var dbConnection = _testContext.DatabaseConnection;
            foreach (var entity in _testContext.InsertedEntities.OfType<SimpleBenchmarkEntity>())
            {
                _testContext.QueriedEntities.Add(SimpleCrud.Get<SimpleBenchmarkEntity>(dbConnection, entity.Id));
            }
        }

        [When(@"I update all the benchmark entities that I previously inserted using Simple Crud")]
        public void WhenIUpdateAllTheSingleIntKeyEntitiesThatIPreviouslyInsertedUsingSimpleCrud()
        {
            var dbConnection = _testContext.DatabaseConnection;
            var entityIndex = _testContext.InsertedEntities.Count;

            foreach (var entity in _testContext.InsertedEntities.OfType<SimpleBenchmarkEntity>())
            {
                var newEntity = this.GenerateSimpleBenchmarkEntity(entityIndex++);
                newEntity.Id = entity.Id;
                SimpleCrud.Update(dbConnection, newEntity);
                _testContext.UpdatedEntities.Add(newEntity);
            }
        }

        [When(@"I delete all the inserted benchmark entities using Simple Crud")]
        public void WhenIDeleteAllTheInsertedSingleIntKeyEntitiesUsingSimpleCrud()
        {
            var dbConnection = _testContext.DatabaseConnection;

            foreach (var entity in _testContext.InsertedEntities.OfType<SimpleBenchmarkEntity>())
            {
                SimpleCrud.Delete(dbConnection, entity);
            }
        }

    }
}