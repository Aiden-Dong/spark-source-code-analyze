/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

var uiRoot = "";

function setUIRoot(val) {
    uiRoot = val;
}

function collapseTablePageLoad(name, table){
  if (window.localStorage.getItem(name) == "true") {
    // Set it to false so that the click function can revert it
    window.localStorage.setItem(name, "false");
    collapseTable(name, table);
  }
}

function collapseTable(thisName, table){
    var status = window.localStorage.getItem(thisName) == "true";
    status = !status;

    thisClass = '.' + thisName

    // Expand the list of additional metrics.
    var tableDiv = $(thisClass).parent().find('.' + table);
    $(tableDiv).toggleClass('collapsed');

    // Switch the class of the arrow from open to closed.
    $(thisClass).find('.collapse-table-arrow').toggleClass('arrow-open');
    $(thisClass).find('.collapse-table-arrow').toggleClass('arrow-closed');

    window.localStorage.setItem(thisName, "" + status);
}

// Add a call to collapseTablePageLoad() on each collapsible table
// to remember if it's collapsed on each page reload
$(function() {
  collapseTablePageLoad('collapse-aggregated-metrics','aggregated-metrics');
  collapseTablePageLoad('collapse-aggregated-executors','aggregated-executors');
  collapseTablePageLoad('collapse-aggregated-removedExecutors','aggregated-removedExecutors');
  collapseTablePageLoad('collapse-aggregated-workers','aggregated-workers');
  collapseTablePageLoad('collapse-aggregated-activeApps','aggregated-activeApps');
  collapseTablePageLoad('collapse-aggregated-activeDrivers','aggregated-activeDrivers');
  collapseTablePageLoad('collapse-aggregated-completedApps','aggregated-completedApps');
  collapseTablePageLoad('collapse-aggregated-completedDrivers','aggregated-completedDrivers');
  collapseTablePageLoad('collapse-aggregated-runningExecutors','aggregated-runningExecutors');
  collapseTablePageLoad('collapse-aggregated-runningDrivers','aggregated-runningDrivers');
  collapseTablePageLoad('collapse-aggregated-finishedExecutors','aggregated-finishedExecutors');
  collapseTablePageLoad('collapse-aggregated-finishedDrivers','aggregated-finishedDrivers');
  collapseTablePageLoad('collapse-aggregated-runtimeInformation','aggregated-runtimeInformation');
  collapseTablePageLoad('collapse-aggregated-sparkProperties','aggregated-sparkProperties');
  collapseTablePageLoad('collapse-aggregated-systemProperties','aggregated-systemProperties');
  collapseTablePageLoad('collapse-aggregated-classpathEntries','aggregated-classpathEntries');
  collapseTablePageLoad('collapse-aggregated-activeJobs','aggregated-activeJobs');
  collapseTablePageLoad('collapse-aggregated-completedJobs','aggregated-completedJobs');
  collapseTablePageLoad('collapse-aggregated-failedJobs','aggregated-failedJobs');
  collapseTablePageLoad('collapse-aggregated-poolTable','aggregated-poolTable');
  collapseTablePageLoad('collapse-aggregated-allActiveStages','aggregated-allActiveStages');
  collapseTablePageLoad('collapse-aggregated-allPendingStages','aggregated-allPendingStages');
  collapseTablePageLoad('collapse-aggregated-allCompletedStages','aggregated-allCompletedStages');
  collapseTablePageLoad('collapse-aggregated-allSkippedStages','aggregated-allSkippedStages');
  collapseTablePageLoad('collapse-aggregated-allFailedStages','aggregated-allFailedStages');
  collapseTablePageLoad('collapse-aggregated-activeStages','aggregated-activeStages');
  collapseTablePageLoad('collapse-aggregated-pendingOrSkippedStages','aggregated-pendingOrSkippedStages');
  collapseTablePageLoad('collapse-aggregated-completedStages','aggregated-completedStages');
  collapseTablePageLoad('collapse-aggregated-failedStages','aggregated-failedStages');
  collapseTablePageLoad('collapse-aggregated-poolActiveStages','aggregated-poolActiveStages');
  collapseTablePageLoad('collapse-aggregated-tasks','aggregated-tasks');
  collapseTablePageLoad('collapse-aggregated-rdds','aggregated-rdds');
  collapseTablePageLoad('collapse-aggregated-activeBatches','aggregated-activeBatches');
  collapseTablePageLoad('collapse-aggregated-completedBatches','aggregated-completedBatches');
});