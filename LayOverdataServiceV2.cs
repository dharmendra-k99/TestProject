using Clob.AC.Application.Common;
using Clob.AC.Application.Dtos.CrewInfo;
using Clob.AC.Application.Dtos.CrewQualification;
using Clob.AC.Application.Dtos.Meals.CrewMeals;
using Clob.AC.Application.Extensions;
using Clob.AC.Application.IServices;
using Clob.AC.Domain.Models;
using Clob.AC.Domain.ValueObjects;
using Clob.Flight.Domain.Models;
using Microsoft.Extensions.Logging;
using System.Diagnostics.Metrics;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace Clob.AC.Application.Services
{
    public class LayoverDataService(IApplicationDbContext _dbContext, IHttpService _httpService, ILogger<LayoverDataService> _logger) : ILayoverDataService
    {
        private List<StationMasterModel> StationMaster = new();
        private List<AdminConfiguration> ConfigurationSlabs = new();
        private List<AdminConfigCrewRank> CrewRanks = new();
        private List<AdminConfigDutyType> AdminDutyTypes = new();
        private List<AdminConfigNBDutyType> AdminNonDutyTypes = new();
        private List<AdminConfigCity> AdminConfigCities = new();
        private List<AdminConfigCrewCategory> AdminCategory = new();
        private List<AdhocAllowance> AdhocAllowanceConfig = new();
        private List<AdhocCrewRank> AdhocCrewRanks = new();
        private List<AdhocCountry> AdhocCountries = new();
        private List<AdhocCrewCategory> AdhocCrewCategories = new();
        private List<CountryCurrencyDto> CurrenciesData = new();
        private List<LayoverDataV2> createLayoverData = new();
        private List<LayoverDataV2> existingLayoverData = new();
        private List<MealConfigCrewRank> MealCrewRanks = new();
        private List<MealConfigCrewCategory> MealCrewCategoris = new();
        private List<MealConfigCrewQualification> MealCrewQualifications = new();
        private List<MealConfigAircraftType> MealAircraftTypes = new();
        private List<MealConfiguration> MealConfigurations = new();

        public async Task<RosterResponse> GenrateCrewLayoverData(GenerateReportRequestDto request, RosterResponse rosterResponse)
        {
            //Logging
            ReportHistoryModel reportHistoryModel = new ReportHistoryModel()
            {
                FromDate = request.FromDate,
                ToDate = request.ToDate,
                ReportStartTime = DateTime.UtcNow,
                ReportName = CommanConstant.LayoverData_ReportName,
                JobType = request.RosterType,
                Remark = request.Remark,
                Error = null,
                IsSuccess = false
            };

            try
            {
                List<int> categoryIds = new List<int>();


                var fromDate = request.FromDate.AddDays(-1);
                var toDate = request.ToDate;
                var IGA = request.IGA ?? 0;


                var crewInformation = await _dbContext.CrewInformation
                                           .Where(d => (IGA == 0 || d.EmpNo == IGA)
                                           && !(d.IsDeleted ?? false) && (d.IsActive ?? true))
                                              .Select(d => new CrewDetailDto
                                              {
                                                  EmpNo = d.EmpNo,
                                                  FirstName = d.FirstName,
                                                  MiddleName = d.MiddleName,
                                                  LastName = d.LastName,
                                                  PhoneNumber = d.PhoneNumber,
                                                  CrewRank = d.CrewRank,
                                                  PassportExpiry = d.PassportExpiry,
                                                  PassportNumber = d.PassportNumber
                                              })
                                           .Distinct()
                                           .ToListAsync();

                var crewList = _dbContext.CrewLayoverData
                                .Where(c => (IGA == 0 || c.EmpNo == IGA)
                                         && c.Std >= fromDate
                                         && c.Sta <= toDate
                                         )
                                .AsNoTracking()
                                .Select(c => c.EmpNo)
                                .Distinct()
                                .OrderBy(empNo => empNo)
                                .ToList();

                var totalCrewEmp = crewList.Count;


                if (totalCrewEmp > 0 && crewInformation.Count > 0)
                {
                    StationMaster = await _dbContext.StationMasters.Where(x => x.IsActive ?? true).ToListAsync();

                    CurrenciesData = await (
                       from currency in _dbContext.Currency
                       join country in _dbContext.Country
                      on currency.CountryId equals country.CountryName
                       where (currency.IsActive ?? true) && !(currency.IsDeleted ?? false)
                       select new CountryCurrencyDto
                       {
                           CurrencyCode = currency.CurrencyCode ?? "",
                           CountryId = country.CountryID ?? "",
                           ValueInINR = currency.ValueInINR ?? 0,
                           CountryName = country.CountryName ?? "",
                       }).Distinct().ToListAsync();

                    categoryIds = await _dbContext.CrewCategoryData.Select(x => x.CrewCategoryId).ToListAsync();

                    ConfigurationSlabs = await _dbContext.AdminConfiguration.Where(x => x.IsActive == true && x.EffectiveFrom <= fromDate && (x.EffectiveTo == null || x.EffectiveTo >= toDate)).ToListAsync();

                    var configIds = ConfigurationSlabs.Select(c => c.Id.Value).ToList();

                    if (configIds != null && configIds.Any())
                    {
                        CrewRanks = await _dbContext.AdminConfigCrewRank.Where(c => configIds.Contains(c.AdminConfigurationId)).ToListAsync();
                        AdminDutyTypes = await _dbContext.AdminConfigDutyType.Where(c => configIds.Contains(c.AdminConfigurationId)).ToListAsync();
                        AdminNonDutyTypes = await _dbContext.AdminConfigNBDutyType.Where(c => configIds.Contains(c.AdminConfigurationId)).ToListAsync();
                        AdminConfigCities = await _dbContext.AdminConfigCity.Where(c => configIds.Contains(c.AdminConfigurationId)).ToListAsync();
                        AdminCategory = await _dbContext.AdminConfigCrewCategory.Where(c => configIds.Contains(c.AdminConfigurationId)).ToListAsync();



                        MealConfigurations = await _dbContext.MealConfigurations.Where(x => x.IsActive == true && x.EffectiveFrom <= fromDate && (x.EffectiveTo == null || x.EffectiveTo >= toDate)).ToListAsync();


                        var mealConfigIds = MealConfigurations.Select(c => c.Id.Value).ToList();

                        if (mealConfigIds != null && mealConfigIds.Any())
                        {
                            MealCrewRanks = await _dbContext.MealConfigCrewRanks
                                                                      .Where(c => mealConfigIds.Contains(c.MealConfigId))
                                                                      .ToListAsync();

                            MealCrewCategoris = await _dbContext.MealConfigCrewCategories
                                                                         .Where(c => mealConfigIds.Contains(c.MealConfigId))
                                                                         .ToListAsync();

                            MealAircraftTypes = await _dbContext.MealConfigAircraftTypes
                                                                                .Where(c => mealConfigIds.Contains(c.MealConfigId))
                                                                                .ToListAsync();
                        }

                        AdhocAllowanceConfig = await _dbContext.AdhocAllowance.Where(x => x.IsActive == true && x.EffectiveFrom <= fromDate && (x.EffectiveTo == null || x.EffectiveTo >= toDate)).ToListAsync();

                        var adhocIds = AdhocAllowanceConfig.Select(c => c.Id.Value).ToList();

                        if (adhocIds != null && adhocIds.Any())
                        {
                            AdhocCrewRanks = await _dbContext.AdhocCrewRank.Where(c => adhocIds.Contains(c.AdhocId)).ToListAsync();
                            AdhocCountries = await _dbContext.AdhocCountry.Where(c => adhocIds.Contains(c.AdhocId)).ToListAsync();
                            AdhocCrewCategories = await _dbContext.AdhocCrewCategory.Where(c => adhocIds.Contains(c.AdhocId)).ToListAsync();
                        }
                    }


                    int batchSize = 5000;
                    int recordsToSkip = 0;

                    while (recordsToSkip < totalCrewEmp)
                    {
                        var recordToProcess = Math.Min(batchSize, totalCrewEmp - recordsToSkip);

                        var batchEmplst = crewList
                            .Skip(recordsToSkip)
                            .Take(recordToProcess)
                            .ToList();

                        var rowStartDt = DateTime.Now;

                        Console.WriteLine($"Layover Report crewLayoverData fetching at Timestamp = {rowStartDt.TimeOfDay}");

                        var crewLayoverData = await _dbContext.CrewLayoverData
                          .Where(c => batchEmplst.Contains(c.EmpNo) && c.Std >= fromDate && c.Sta <= toDate)
                          .Select(c => new CrewLayoverRoaster
                          {
                              EmpNo = c.EmpNo,
                              Dep = c.Dep,
                              Arr = c.Arr,
                              ActDep = c.ActDep,
                              ActArr = c.ActArr,
                              Std = c.Std,
                              Sta = c.Sta,
                              Flt = c.Flt,
                              DutyType = c.DutyType,
                              CrewBase = c.CrewBase,
                              AirCraftType = c.AirCraftType
                          })
                          .AsNoTracking()
                          .OrderBy(c => c.EmpNo)
                          .ThenBy(c => c.Std)
                          .ToListAsync();

                        Console.WriteLine($"Layover Report crewLayoverData fetched at Timestamp = {DateTime.Now.TimeOfDay} in Total Duration : {(DateTime.Now - rowStartDt).TotalSeconds} seconds.");

                        if (crewLayoverData.Count > 0)
                        {
                            await GenrateLayoverHoursData(crewLayoverData, crewInformation, rosterResponse, request.FromDate, categoryIds);
                        }


                        recordsToSkip += recordToProcess;
                    }
                }
            }
            catch (Exception ex)
            {
                rosterResponse.Error = ex.Message;
                rosterResponse.Status = false;
                rosterResponse.Stacktrace = ex.StackTrace;
                _logger.LogError("LayoverDataCalculation: Error occured " + ex.ToString() + ex.StackTrace);
            }
            finally
            {
                //logging
                await AddReportHistory(reportHistoryModel, rosterResponse);
            }

            return rosterResponse;
        }

        private async Task GenrateLayoverHoursData(List<CrewLayoverRoaster> crewLayoverData, List<CrewDetailDto> crewInformationData, RosterResponse rosterResponse, DateTime fromDate, List<int> categoryIds)
        {
            var grouped = crewLayoverData.GroupBy(f => f.EmpNo);
            int counter = 0;
            Console.WriteLine($"Total employee  Count in GenrateLayoverHoursData: {grouped.Count()}");
            try
            {
                foreach (var group in grouped)
                {

                    var rowStartDt = DateTime.Now;

                    Console.WriteLine($"Loop Count: {++counter} Emp No: {group.Key}");
                    Console.WriteLine($"Layover Report Genration start For empNo = {group.Key} at Timestamp = {rowStartDt.TimeOfDay}");


                    var crewInfo = crewInformationData.Where(x => x.EmpNo == group.Key).FirstOrDefault();
                    if (crewInfo == null)
                    {
                        continue;
                    }
                    await CalculateLayoverHours(group, crewInfo, categoryIds, fromDate);

                    Console.WriteLine($"Layover Report Generated For empNo =  {group.Key} at Timestamp: {DateTime.Now.TimeOfDay} in Total Duration : {(DateTime.Now - rowStartDt).TotalSeconds} seconds :: Records Processed {rosterResponse.RowsInserted}");
                }

                var transactionTime = DateTime.Now;

                Console.WriteLine($"Records adding transaction begin at -{transactionTime.TimeOfDay}");

                using var transaction = _dbContext.Database.BeginTransaction();

                if (existingLayoverData != null && existingLayoverData.Any())
                {
                    Console.WriteLine($"Records deleting to table-{DateTime.Now.TimeOfDay}");
                    // Extract secondary entities
                    var existingCrewMeals = existingLayoverData.Where(p => p.CrewMealReport != null)
                                                           .Select(p => p.CrewMealReport)
                                                           .ToList();

                    var existingLayoverReport = existingLayoverData.Where(p => p.LayoverReport != null)
                                                         .Select(p => p.LayoverReport)
                                                         .ToList();

                    await _dbContext.CrewMealReportV2.BulkDeleteAsync(existingCrewMeals);
                    await _dbContext.InternationalLayoverReport.BulkDeleteAsync(existingLayoverReport);
                    await _dbContext.LayoverDataV2.BulkDeleteAsync(existingLayoverData);
                    existingLayoverData.Clear();
                    Console.WriteLine($"Records deleted to table- {DateTime.Now.TimeOfDay}");
                }

                if (createLayoverData.Any())
                {

                    Console.WriteLine($"Records adding to table-{DateTime.Now.TimeOfDay}");



                    await _dbContext.LayoverDataV2.BulkInsertAsync(createLayoverData);


                    createLayoverData
                        .Where(e => e.CrewMealReport != null)
                        .ToList()
                        .ForEach(e => e.CrewMealReport.LayoverDataId = e.Id);

                    createLayoverData
                      .Where(e => e.LayoverReport != null)
                      .ToList()
                      .ForEach(e => e.LayoverReport.LayoverDataId = e.Id);


                    var entityCrewMeals = createLayoverData.Where(p => p.CrewMealReport != null)
                                                            .Select(p => p.CrewMealReport)
                                                            .ToList();

                    var entityLayoverReport = createLayoverData.Where(p => p.LayoverReport != null)
                                                         .Select(p => p.LayoverReport)
                                                         .ToList();

                    await _dbContext.CrewMealReportV2.BulkInsertAsync(entityCrewMeals);
                    await _dbContext.InternationalLayoverReport.BulkInsertAsync(entityLayoverReport);

                    Console.WriteLine($"Records added to table-{DateTime.Now.TimeOfDay}");
                    Console.WriteLine($"Save to database started-{DateTime.Now.TimeOfDay}");

                    await _dbContext.SaveChangesAsync();

                    transaction.Commit();

                    Console.WriteLine($"Records adding transaction end at -{DateTime.Now.TimeOfDay}  Duration : {(DateTime.Now - transactionTime).TotalSeconds} seconds.");

                    rosterResponse.Status = true;
                    rosterResponse.RowsInserted += createLayoverData.Count();
                    createLayoverData.Clear();

                    Console.WriteLine($"Save to database stopped-{DateTime.Now.TimeOfDay}");
                }

                rosterResponse.Status = true;
            }
            catch (Exception ex)
            {
                rosterResponse.Error = ex.Message;
                rosterResponse.Status = false;
                rosterResponse.Stacktrace = ex.StackTrace;
                _logger.LogError("LayoverDataCalculation: Error occured " + ex.ToString() + ex.StackTrace);
            }

        }

        private async Task CalculateLayoverHours(IGrouping<long, CrewLayoverRoaster> crewLayoverData, CrewDetailDto crewInfo, List<int> categoryIds, DateTime layOverfromDate)
        {

            bool previousMonthRecordAdded = false;
            var allCrewFlights = crewLayoverData.OrderBy(f => f.Std).ToList();
            var crewFlights = allCrewFlights.Where(x => x.Sta >= layOverfromDate).ToList();
            var crewName = string.Join(" ", new[] { crewInfo?.FirstName, crewInfo?.MiddleName, crewInfo?.LastName }.Where(name => !string.IsNullOrWhiteSpace(name)));
            var crewRank = crewInfo?.CrewRank?.ToUpper() ?? "";
            var IGA = crewInfo?.EmpNo ?? 0;
            var phoneNumber = crewInfo?.PhoneNumber ?? "";
            var passportNumber = crewInfo?.PassportNumber ?? "";
            var passportExpiry = crewInfo?.PassportExpiry;

            var mealCrewRanks = MealCrewRanks
                                          .Where(d => d.CrewRankName?.ToUpper() == crewRank)
                                          .Select(m => m.MealConfigId)
                                          .ToList();

            var mealCategories = MealCrewCategoris
                           .Where(d => categoryIds.Contains(d.CategoryId))
                           .Select(m => m.MealConfigId)
                            .ToList();

            if (crewFlights.Count > 0)
            {
                var previousMonthLastDayFlights = allCrewFlights.Where(x => x.Sta < layOverfromDate).OrderBy(f => f.Std).LastOrDefault();

                var lastDayPreviousMonth = layOverfromDate.AddDays(-1);
                var firstDutyOfMonth = crewFlights[0];
                var isDutyStartFromNonBase = (firstDutyOfMonth.Dep != string.Empty && firstDutyOfMonth.CrewBase != string.Empty) && firstDutyOfMonth.Dep != firstDutyOfMonth.CrewBase;
                if (isDutyStartFromNonBase && crewFlights.Count > 0 && previousMonthLastDayFlights != null)
                {
                    crewFlights.Add(previousMonthLastDayFlights);
                    crewFlights = crewFlights.OrderBy(f => f.Std).ToList();
                    previousMonthRecordAdded = true;
                }
            }
            for (int i = 0; i < crewFlights.Count; i++)
            {
                string currentStay = string.Empty;
                int layoverStayHours = 0;
                int layoverStayMinutes = 0;

                var current = crewFlights[i];
                var next = i == crewFlights.Count - 1 ? null : crewFlights[i + 1];
                if (current.Arr == next?.Arr && current.Dep == next?.Dep && current.ActArr == next?.ActArr && current.ActDep == next?.ActDep)
                {
                    continue; //Skip duplicate record
                }

                if (current.Arr != string.Empty)
                {
                    currentStay = current.Arr?.Trim() ?? "";
                }

                var stationData = StationMaster?.Where(x => x.StationCode == currentStay).FirstOrDefault();

                bool isActArrValidDate = current.ActArr.Year > 2000;
                bool isActDepValidDate = current.ActDep.Year > 2000;
                bool isNextActDepValidDate = next?.ActDep.Year > 2000;

                bool isActArrFutureDate = current.ActArr > DateTime.Now;
                bool isActDepFutureDate = current.ActDep > DateTime.Now;
                bool isNextActDepFutureDate = next?.ActDep > DateTime.Now;

                bool isActualData = false;


                DateTime dutyStart;
                DateTime stayStart;
                DateTime? stayEnd;
                if (!(isActDepFutureDate || isActArrFutureDate || isNextActDepFutureDate) && (isActDepValidDate && isActArrValidDate && isNextActDepValidDate))
                {
                    dutyStart = current.ActDep;
                    stayStart = current.ActArr;
                    stayEnd = next?.ActDep;
                    isActualData = true;
                }
                else
                {
                    dutyStart = current.Std;
                    stayStart = current.Sta;
                    stayEnd = next?.Std;
                }


                bool isCrewBase = currentStay == current.CrewBase.Trim();
                var aircraftTypeIds = current.AirCraftType?.Trim() ?? "";
                var inboundFlight = current.Flt > 0 ? current.Flt.ToStr() : "";
                var outBoundFlight = next?.Flt > 0 ? next?.Flt.ToStr() : "";
                var deptStation = current.Dep;
                var arrStation = current.Arr;


                if (current.Dep != current.Arr && !(i == 0 && previousMonthRecordAdded))
                {
                    var layoverTotalTimeInMinutes = 0.0;

                    if (stayEnd != null)
                    {
                        layoverTotalTimeInMinutes = (Convert.ToDateTime(stayEnd) - stayStart).TotalMinutes;
                        layoverStayHours = Convert.ToInt32(layoverTotalTimeInMinutes / 60);
                        layoverStayMinutes = Convert.ToInt32(layoverTotalTimeInMinutes % 60);
                    }
                }

                if (!string.IsNullOrWhiteSpace(inboundFlight)
                    && !string.IsNullOrWhiteSpace(outBoundFlight)
                    && !string.IsNullOrWhiteSpace(deptStation)
                    && !string.IsNullOrWhiteSpace(arrStation))
                {
                    var crewCategory = string.Join(",", categoryIds.Distinct().Select(x => x));


                    var currentDutyType = current?.DutyType?.Trim() ?? "";
                    var nextDutyType = next?.DutyType?.Trim() ?? "";
                    var countryId = stationData?.Country ?? "";
                    var restaurant = string.Empty;
                    decimal mealAllowance = 0;
                    var isMealConfigExist = false;

                    double stayHours = Convert.ToDouble(layoverStayHours);
                    double stayMinutes = Convert.ToDouble(layoverStayMinutes);

                    var (crewAdminConfigSlabs, targetTotalhours) = GetCrewAdminConfigSlabs(stayHours, stayMinutes, isActualData, crewRank, currentDutyType, nextDutyType, categoryIds, currentStay, stayStart.Date);

                    decimal layoverAllowance = 0;

                    if (!isCrewBase)
                    {
                        layoverAllowance = CalculateAllowance(crewAdminConfigSlabs, targetTotalhours, isActualData, false);

                        if (layoverAllowance > 0)
                        {
                            layoverAllowance += CalculateAdhocAllowance(crewRank, categoryIds, countryId, stayStart.Date, targetTotalhours);
                        }

                        isMealConfigExist = crewAdminConfigSlabs.Any(d => d.IsCrewMealProvided);

                        if (isMealConfigExist)
                        {
                            //var flightDtailsRequest = new FlightDetailsRequestDto
                            //{
                            //    flightNumber = current?.Flt,
                            //    startDateTime = dutyStart.ToString("s") ?? null,
                            //    endDateTime = stayStart.ToString("s") ?? null,
                            //    endStation = current?.Arr ?? null,
                            //};

                            //var flightResponse = new FlightDocument
                            //{
                            //    FlightLegState = new FlightLegState()
                            //};

                            //var flightApiResponse = await _httpService.GetFlightDetailsAsync<FlightDetailApiResponseRoot>(flightDtailsRequest);
                            //if (flightApiResponse != null)
                            //{
                            //    flightResponse = flightApiResponse.FlightDocumentList.FirstOrDefault();
                            //}

                            //var flightDetails = flightResponse?.FlightLegState;
                            //var terminalNo = string.IsNullOrWhiteSpace(flightDetails?.EndTerminal) ? "1" : flightDetails.EndTerminal;

                            var terminalNo = "1";
                            var crewAircraftTypeIdlst = aircraftTypeIds.Split(",").Select(ac => ac.Trim()).ToList();
                            var mealAircraftTypes = MealAircraftTypes
                                                                    .Where(d => crewAircraftTypeIdlst.Contains(d.AircraftTypeId ?? ""))
                                                                    .Select(m => m.MealConfigId)
                                                                     .ToList();

                            var fromTime = stayStart.TimeOfDay;
                            var toTime = stayEnd?.Date.TimeOfDay;


                            var mealConfig = MealConfigurations.Where(m => m.EffectiveFrom <= stayStart.Date &&
                                                                      m.EffectiveTo >= stayEnd?.Date &&
                                                                      m.OpeningTime <= fromTime &&
                                                                      (m.ClosingTime.HasValue && (m.ClosingTime.Value.Add(TimeSpan.FromMinutes(m.BaseDepartureTimeDelay))) >= toTime) &&
                                                                      // Ensure the time range does NOT overlap with blackout period
                                                                      !(fromTime < m.BlackoutEndTime && toTime > m.BlackoutStartTime) &&
                                                                      mealCategories.Contains(m.Id.Value) &&
                                                                      mealCrewRanks.Contains(m.Id.Value) &&
                                                                      mealAircraftTypes.Contains(m.Id.Value) &&
                                                                      //mealCrewQualifications.Contains(m.Id.Value) &&
                                                                      m.Station == currentStay &&
                                                                      m.TerminalNo == terminalNo
                                                                      )
                                                                      .OrderBy(m => m.PriorityIndex)
                                                                      .FirstOrDefault();

                            if (mealConfig != null)
                            {
                                restaurant = mealConfig.Restaurant;
                                mealAllowance = 0;
                            }
                            else
                            {
                                mealAllowance = CalculateAllowance(crewAdminConfigSlabs, targetTotalhours, isActualData, true);
                            }

                        }
                    }

                    var currency = stationData?.Country == "IN" ? "INR" : "USD";

                    var layoverCurency = CurrenciesData?.Where(d => d.CountryId == stationData?.Country).FirstOrDefault();

                    if (layoverCurency != null)
                    {
                        currency = layoverCurency.CurrencyCode;
                        layoverAllowance = layoverAllowance * layoverCurency.ValueInINR;
                        mealAllowance = mealAllowance * layoverCurency.ValueInINR;
                    }

                    var sta = current?.Sta.TimeOfDay.ToStr();



                    var creatData = new LayoverDataV2
                    {
                        InboundFlight = inboundFlight,
                        OutboundFlight = outBoundFlight,
                        DepartureStation = deptStation,
                        ArrivalStation = arrStation,
                        StayStart = stayStart,
                        StayEnd = stayEnd,
                        LayoverHours = layoverStayHours,
                        LayoverMinutes = layoverStayMinutes,
                        IGA = IGA,
                        CrewRank = crewRank,
                        CrewCategory = crewCategory,
                        CrewQualification = aircraftTypeIds,
                        AirCraftType = aircraftTypeIds,
                        CrewName = crewName,
                        PhoneNumber = phoneNumber,
                        Date = stayStart.Date,
                        IsActualData = isActualData,
                        IsCrewBase = isCrewBase,
                        Currency = currency,
                        Country = countryId,
                        CrewBase = current?.CrewBase.Trim() ?? "",
                        PassportNumber = passportNumber,
                        PassportExpiry = passportExpiry,
                        STA = sta,
                        IsDomestic = countryId.ToUpper() == "IN",
                    };


                    var createLayoverReport = new InternationalLayoverReport
                    {
                        Allowance = layoverAllowance
                    };


                    var createCrewMealReport = new CrewMealReportV2
                    {
                        Restaurant = restaurant,
                        MealAllowance = mealAllowance
                    };

                    await CreateLayoverData(creatData, createLayoverReport, createCrewMealReport);

                }
            }
        }

        private (List<AdminConfiguration>, double) GetCrewAdminConfigSlabs(double layoverHours, double layoverMinutes, bool isActualData, string crewRank, string currentDutyType, string nextDutyType, List<int> categoryIds, string cityId, DateTime date)
        {
            var slabs = ConfigurationSlabs.Where(x => x.EffectiveFrom <= date && (x.EffectiveTo == null || x.EffectiveTo >= date)).ToList();
            var slabIds = slabs.Select(y => y.Id.Value).ToList();
            var sortdByRanksIds = CrewRanks.Where(cr => slabIds.Contains(cr.AdminConfigurationId) && cr.CrewRankName?.ToUpper() == crewRank).Select(x => x.AdminConfigurationId).ToList();
            var sortedByDutyTypeIds = AdminDutyTypes.Where(dt => sortdByRanksIds.Contains(dt.AdminConfigurationId) && dt.DutyType == currentDutyType).Select(x => x.AdminConfigurationId).ToList();
            var sortedByNonDutyTypeIds = AdminNonDutyTypes.Where(dt => sortdByRanksIds.Contains(dt.AdminConfigurationId) && dt.NBDutyTypeName == currentDutyType).Select(x => x.AdminConfigurationId).ToList();
            var sortedIds = sortedByDutyTypeIds.Count() > 0 ? sortedByDutyTypeIds : sortedByNonDutyTypeIds;
            var sortedByCitiesIds = AdminConfigCities.Where(ct => sortedIds.Contains(ct.AdminConfigurationId) && ct.CityId == cityId).Select(x => x.AdminConfigurationId).ToList();
            var sortedByCategoryIds = AdminCategory.Where(ct => sortedByCitiesIds.Contains(ct.AdminConfigurationId) && categoryIds.Contains(ct.CategoryId)).Select(x => x.AdminConfigurationId).Distinct().ToList();
            var crewategoryIds = AdminCategory.Where(ct => sortedByCitiesIds.Contains(ct.AdminConfigurationId) && categoryIds.Contains(ct.CategoryId)).Select(x => x.CategoryId).Distinct().ToList();
            var nonDutyTypeIds = AdminNonDutyTypes.Where(dt => sortedByCitiesIds.Contains(dt.AdminConfigurationId) && dt.NBDutyTypeName == currentDutyType).Select(x => x.NBDutyTypeName).Distinct().ToList();
            var nextNonDutyTypesIds = AdminNonDutyTypes.Where(dt => sortedByCitiesIds.Contains(dt.AdminConfigurationId) && dt.NBDutyTypeName == nextDutyType).Select(x => x.NBDutyTypeName).Distinct().ToList();
            var filterdSlabs = slabs.Where(x => sortedByCategoryIds.Contains((long)x.Id.Value)).OrderBy(s => s.DurationFromHours).ToList();


            double targetTotalhours = layoverMinutes / 60 + layoverHours;

            var finalSlabs = filterdSlabs.Where(slab =>
                    (slab.DurationFromMinutes / 60 + slab.DurationFromHours) <= targetTotalhours
                    &&
                    (
                    (targetTotalhours <= (slab.DurationToMinutes / 60 + slab.DurationToHours))
                    ||
                    (isActualData && targetTotalhours <= ((slab.DurationToMinutes / 60 + slab.DurationToHours) + (slab.HourlySlabMinutes / 60 + slab.HourlySlabHours)))
                    )
                    ).ToList();

            return (finalSlabs, targetTotalhours);
        }


        static decimal CalculateAllowance(List<AdminConfiguration> adminConfigSlabs, double targetTotalhours, bool isActualData, bool isMealConfig)
        {

            decimal totalAllowance = 0;

            var availableSlabs = adminConfigSlabs
                .Where(slab => slab.IsCrewMealProvided == isMealConfig).FirstOrDefault();


            if (availableSlabs != null)
            {
                totalAllowance = availableSlabs.Allowance;

                double toHours = availableSlabs.DurationToMinutes / 60 + availableSlabs.DurationToHours;
                double slabCapHours = availableSlabs.HourlySlabMinutes / 60 + availableSlabs.HourlySlabHours;

                if (isActualData && targetTotalhours > toHours)
                {
                    double extraHours = targetTotalhours - toHours;

                    double cappedExtra = Math.Min(extraHours, slabCapHours);

                    int roundedExtra = (int)Math.Ceiling(cappedExtra);

                    totalAllowance += roundedExtra * availableSlabs.PerHourAllowance;
                }
            }

            return totalAllowance;
        }


        private decimal CalculateAdhocAllowance(string crewRank, List<int> categoryIds, string countryId, DateTime date, double targetTotalhours)
        {

            var slabs = AdhocAllowanceConfig.Where(x => x.EffectiveFrom <= date && (x.EffectiveTo == null || x.EffectiveTo >= date)).ToList();
            var slabIds = slabs.Select(y => y.Id.Value).ToList();
            var sortdByRanksIds = AdhocCrewRanks.Where(cr => slabIds.Contains(cr.AdhocId) && cr.CrewRankName == crewRank).Select(x => x.AdhocId).ToList();
            var sortedByCountryIds = AdhocCountries.Where(ct => sortdByRanksIds.Contains(ct.AdhocId) && ct.CountryId == countryId).Select(x => x.AdhocId).ToList();
            var sortedByCategoryIds = AdhocCrewCategories.Where(ct => sortedByCountryIds.Contains(ct.AdhocId) && categoryIds.Contains(ct.CategoryId)).Select(x => x.AdhocId).Distinct().ToList();
            var adhocSlab = slabs.Where(x => sortedByCategoryIds.Contains((int)x.Id.Value)).OrderBy(s => s.PerHourSlabHours).FirstOrDefault();

            decimal totalAllowance = 0;

            if (adhocSlab != null)
            {
                var adhocTime = (int)Math.Ceiling(targetTotalhours / adhocSlab.PerHourSlabHours);
                totalAllowance = adhocTime * adhocSlab?.Amount ?? 0;
            }

            return totalAllowance;
        }

        private async Task CreateLayoverData(LayoverDataV2 layoverData, InternationalLayoverReport layoverReport, CrewMealReportV2 crewMealReport)
        {
            layoverData.CreatedBy = 1234;
            layoverData.CreatedAt = DateTime.UtcNow;
            layoverData.IsActive = true;

            layoverReport.CreatedBy = 1234;
            layoverReport.CreatedAt = DateTime.UtcNow;
            layoverReport.IsActive = true;
            layoverReport.Status = ReportConstant.StatusPending;

            crewMealReport.CreatedBy = 1234;
            crewMealReport.CreatedAt = DateTime.UtcNow;
            crewMealReport.IsActive = true;
            crewMealReport.Status = ReportConstant.StatusPending;

            // Check for duplicate before adding
            bool isDuplicate = createLayoverData.Any(x =>
                x.IGA == layoverData.IGA &&
                x.InboundFlight == layoverData.InboundFlight &&
                x.OutboundFlight == layoverData.OutboundFlight &&
                x.DepartureStation == layoverData.DepartureStation &&
                x.ArrivalStation == layoverData.ArrivalStation &&
                x.StayStart == layoverData.StayStart &&
                x.StayEnd == layoverData.StayEnd);

            if (!isDuplicate)
            {
                if (!layoverData.IsDomestic && layoverReport.Allowance > 0)
                    layoverData.LayoverReport = layoverReport;

                if (!string.IsNullOrWhiteSpace(crewMealReport.Restaurant) || crewMealReport.MealAllowance > 0)
                    layoverData.CrewMealReport = crewMealReport;

                createLayoverData.Add(layoverData);

            }

            var existingReport = await _dbContext.LayoverDataV2.Where(x =>
             x.IGA == layoverData.IGA &&
             x.InboundFlight == layoverData.InboundFlight &&
             x.OutboundFlight == layoverData.OutboundFlight &&
             x.DepartureStation == layoverData.DepartureStation &&
             x.ArrivalStation == layoverData.ArrivalStation &&
             x.StayStart == layoverData.StayStart &&
             x.StayEnd == layoverData.StayEnd).FirstOrDefaultAsync();

            if (existingReport != null)
            {
                existingLayoverData.Add(existingReport);
            }
        }

        /// <summary>
        /// Add report history
        /// </summary>
        /// <param name="reportHistoryModel"></param>
        /// <param name="rosterResponse"></param>
        /// <returns></returns>
        private async Task AddReportHistory(ReportHistoryModel reportHistoryModel, RosterResponse rosterResponse)
        {
            try
            {
                reportHistoryModel.Error = rosterResponse.Error != null ? rosterResponse.Error + " -Stacktrace " + rosterResponse.Stacktrace : null;
                reportHistoryModel.IsSuccess = rosterResponse.Status;
                reportHistoryModel.ReportEndTime = DateTime.UtcNow;
                reportHistoryModel.UpdatedCount = rosterResponse.RowsUpdated;
                reportHistoryModel.InsertedCount = rosterResponse.RowsInserted;

                _dbContext.ReportHistory.Add(reportHistoryModel);
                await _dbContext.SaveChangesAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error while saving log-" + ex.ToString());
                _logger.LogError("LayoverDataCalculation: Error occured saving report history" + ex.ToString() + ex.StackTrace);
            }
        }

    }
}
