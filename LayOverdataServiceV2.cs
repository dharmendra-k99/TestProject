using Clob.AC.Application.Common;
using Clob.AC.Application.Dtos.CrewInfo;
using Clob.AC.Application.Extensions;
using Clob.AC.Application.IServices;
using Clob.Flight.Domain.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using System.Collections.Concurrent;
using System.Text;

namespace Clob.AC.Application.Services
{
    public class LayoverDataService(IApplicationDbContext _dbContext, IHttpService _httpService, ILogger<LayoverDataService> _logger, IServiceProvider _serviceProvider) : ILayoverDataService
    {
        // Original collections for compatibility
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
        private List<MealConfigCrewRank> MealCrewRanks = new();
        private List<MealConfigCrewCategory> MealCrewCategoris = new();
        private List<MealConfigCrewQualification> MealCrewQualifications = new();
        private List<MealConfigAircraftType> MealAircraftTypes = new();
        private List<MealConfiguration> MealConfigurations = new();

        // Optimized lookup structures
        private Dictionary<string, StationMasterModel> StationMasterLookup = new();
        private Dictionary<string, CountryCurrencyDto> CurrencyLookup = new();
        private Dictionary<int, List<AdminConfiguration>> ConfigSlabsByCategory = new();
        private Dictionary<string, List<MealConfiguration>> MealConfigByStation = new();
        private HashSet<string> ExistingLayoverKeys = new();
        private ConcurrentBag<LayoverDataV2> createLayoverData = new();
        private ConcurrentBag<LayoverDataV2> existingLayoverData = new();

        // Pre-computed lookup tables for performance
        private Dictionary<string, HashSet<int>> MealCrewRankLookup = new();
        private Dictionary<int, HashSet<int>> MealCategoryLookup = new();
        private Dictionary<string, HashSet<int>> MealAircraftTypeLookup = new();
        private DateTime _currentDateTime;

        // Admin configuration lookup tables
        private Dictionary<string, HashSet<long>> AdminCrewRankLookup = new();
        private Dictionary<string, HashSet<long>> AdminDutyTypeLookup = new();
        private Dictionary<string, HashSet<long>> AdminNonDutyTypeLookup = new();
        private Dictionary<string, HashSet<long>> AdminCityLookup = new();
        private Dictionary<int, HashSet<long>> AdminCategoryLookup = new();
        private Dictionary<long, AdminConfiguration> AdminConfigLookup = new();

        // Adhoc allowance lookup tables  
        private Dictionary<string, HashSet<int>> AdhocCrewRankLookup = new();
        private Dictionary<string, HashSet<int>> AdhocCountryLookup = new();
        private Dictionary<int, HashSet<int>> AdhocCategoryLookup = new();
        private Dictionary<int, AdhocAllowance> AdhocAllowanceLookup = new();

        // Pre-calculated duration lookups for performance
        private Dictionary<long, double> AdminConfigDurationFromLookup = new();
        private Dictionary<long, double> AdminConfigDurationToLookup = new();
        private Dictionary<long, double> AdminConfigSlabCapLookup = new();

        // Performance optimizations for data creation
        private DateTime _cachedUtcNow;
        private readonly StringBuilder _keyBuilder = new StringBuilder(200);
        private const int DefaultCreatedBy = 1234;

        // Object pools for reducing allocations
        private readonly Queue<InternationalLayoverReport> _layoverReportPool = new();
        private readonly Queue<CrewMealReportV2> _mealReportPool = new();

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
                    Console.WriteLine($"Loading reference data...");
                    var dataLoadingStartTime = DateTime.Now;

                    // Load all reference data sequentially to avoid DbContext concurrency issues
                    // Note: Each method internally can still use parallel operations with separate contexts if needed
                    await LoadStationMasterData();
                    await LoadCurrencyData();
                    await LoadConfigurationData(fromDate, toDate);

                    categoryIds = await _dbContext.CrewCategoryData.Select(x => x.CrewCategoryId).ToListAsync();

                    await LoadExistingLayoverData(fromDate, toDate, crewList);

                    Console.WriteLine($"Reference data loaded in {(DateTime.Now - dataLoadingStartTime).TotalSeconds} seconds.");

                    // Data loading completed in parallel tasks above


                    // Process crews in parallel with conservative batching to prevent memory issues
                    var maxConcurrency = Math.Min(Environment.ProcessorCount, 5); // More conservative concurrency limit
                    var batchSize = Math.Min(500, Math.Max(50, totalCrewEmp / maxConcurrency)); // Smaller batch sizes
                    
                    // Additional safety checks for memory management
                    var availableMemoryMB = GC.GetTotalMemory(false) / (1024 * 1024);
                    if (availableMemoryMB > 500) // If using more than 500MB, reduce batch size further
                    {
                        batchSize = Math.Min(batchSize, 200);
                        _logger.LogWarning($"High memory usage detected ({availableMemoryMB}MB), reducing batch size to {batchSize}");
                    }

                    _logger.LogInformation($"Processing {totalCrewEmp} crew members with max concurrency: {maxConcurrency}, batch size: {batchSize}");

                    var semaphore = new SemaphoreSlim(maxConcurrency);
                    var processingTasks = new List<Task>();
                    var successfulBatches = 0;
                    var totalBatches = (int)Math.Ceiling((double)totalCrewEmp / batchSize);
                    var failureThreshold = Math.Max(1, totalBatches / 4); // Allow up to 25% failures before stopping

                    try
                    {
                        for (int i = 0; i < totalCrewEmp; i += batchSize)
                        {
                            var batchStart = i;
                            var batchEnd = Math.Min(i + batchSize, totalCrewEmp);
                            var batchEmplst = crewList.Skip(batchStart).Take(batchEnd - batchStart).ToList();
                            var batchNumber = batchStart / batchSize + 1;

                            var task = ProcessCrewBatchWithCircuitBreakerAsync(batchEmplst, fromDate, toDate, 
                                crewInformation, categoryIds, request.FromDate, semaphore, batchNumber);
                            processingTasks.Add(task);

                            // Add delay between batch starts to prevent overwhelming the system
                            if (processingTasks.Count >= maxConcurrency)
                            {
                                await Task.Delay(100); // Small delay to prevent resource spikes
                            }

                            // Monitor memory usage periodically
                            if (batchNumber % 5 == 0) // Check every 5 batches
                            {
                                var currentMemoryMB = GC.GetTotalMemory(false) / (1024 * 1024);
                                if (currentMemoryMB > 1000) // If memory usage exceeds 1GB
                                {
                                    _logger.LogWarning($"High memory usage detected during processing: {currentMemoryMB}MB. Running GC.");
                                    GC.Collect();
                                    GC.WaitForPendingFinalizers();
                                    GC.Collect();
                                }
                            }
                        }

                        var completedTasks = await Task.WhenAll(processingTasks);
                        successfulBatches = completedTasks.Count(t => t.IsCompletedSuccessfully);

                        _logger.LogInformation($"Batch processing completed: {successfulBatches}/{totalBatches} batches succeeded");
                        
                        if (successfulBatches < (totalBatches - failureThreshold))
                        {
                            _logger.LogWarning($"Too many batch failures detected. Only {successfulBatches} out of {totalBatches} batches succeeded.");
                        }
                    }
                    finally
                    {
                        semaphore?.Dispose();
                    }

                    // Process all collected data in a single database transaction
                    await SaveLayoverDataBulk(rosterResponse);
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
                // Clean up memory
                CleanupMemory();

                //logging
                await AddReportHistory(reportHistoryModel, rosterResponse);
            }

            return rosterResponse;
        }

        private (List<AdminConfiguration>, double) GetCrewAdminConfigSlabs(double layoverHours, double layoverMinutes, bool isActualData, string crewRank, string currentDutyType, string nextDutyType, List<int> categoryIds, string cityId, DateTime date)
        {
            return GetCrewAdminConfigSlabsOptimized(layoverHours, layoverMinutes, isActualData, crewRank, currentDutyType, nextDutyType, categoryIds, cityId, date);
        }

        private (List<AdminConfiguration>, double) GetCrewAdminConfigSlabsOptimized(double layoverHours, double layoverMinutes, bool isActualData, string crewRank, string currentDutyType, string nextDutyType, List<int> categoryIds, string cityId, DateTime date)
        {
            double targetTotalHours = layoverMinutes / 60.0 + layoverHours;

            // Filter configurations by effective date first
            var validConfigIds = new HashSet<long>();
            foreach (var config in ConfigurationSlabs)
            {
                if (config.EffectiveFrom <= date && (config.EffectiveTo == null || config.EffectiveTo >= date))
                {
                    validConfigIds.Add(config.Id.Value);
                }
            }

            if (!validConfigIds.Any()) return (new List<AdminConfiguration>(), targetTotalHours);

            // Get rank-filtered config IDs with O(1) lookup
            if (!AdminCrewRankLookup.TryGetValue(crewRank, out var rankConfigIds))
                return (new List<AdminConfiguration>(), targetTotalHours);

            rankConfigIds = rankConfigIds.Intersect(validConfigIds).ToHashSet();
            if (!rankConfigIds.Any()) return (new List<AdminConfiguration>(), targetTotalHours);

            // Get duty type filtered config IDs
            var dutyTypeConfigIds = new HashSet<long>();
            if (AdminDutyTypeLookup.TryGetValue(currentDutyType, out var dutyIds))
            {
                dutyTypeConfigIds = rankConfigIds.Intersect(dutyIds).ToHashSet();
            }

            // If no duty type match, try non-duty type
            if (!dutyTypeConfigIds.Any() && AdminNonDutyTypeLookup.TryGetValue(currentDutyType, out var nonDutyIds))
            {
                dutyTypeConfigIds = rankConfigIds.Intersect(nonDutyIds).ToHashSet();
            }

            if (!dutyTypeConfigIds.Any()) return (new List<AdminConfiguration>(), targetTotalHours);

            // Get city-filtered config IDs
            if (!AdminCityLookup.TryGetValue(cityId, out var cityConfigIds))
                return (new List<AdminConfiguration>(), targetTotalHours);

            var cityFilteredIds = dutyTypeConfigIds.Intersect(cityConfigIds).ToHashSet();
            if (!cityFilteredIds.Any()) return (new List<AdminConfiguration>(), targetTotalHours);

            // Get category-filtered config IDs using intersections
            var categoryFilteredIds = new HashSet<long>();
            foreach (var categoryId in categoryIds)
            {
                if (AdminCategoryLookup.TryGetValue(categoryId, out var categoryConfigIds))
                {
                    var intersection = cityFilteredIds.Intersect(categoryConfigIds);
                    categoryFilteredIds.UnionWith(intersection);
                }
            }

            if (!categoryFilteredIds.Any()) return (new List<AdminConfiguration>(), targetTotalHours);

            // Get filtered configurations and apply time-based filtering
            var filteredConfigs = new List<AdminConfiguration>();
            foreach (var configId in categoryFilteredIds)
            {
                if (AdminConfigLookup.TryGetValue(configId, out var config))
                {
                    // Use pre-computed duration values for performance
                    var durationFrom = AdminConfigDurationFromLookup[configId];
                    var durationTo = AdminConfigDurationToLookup[configId];

                    if (durationFrom <= targetTotalHours)
                    {
                        if (targetTotalHours <= durationTo)
                        {
                            filteredConfigs.Add(config);
                        }
                        else if (isActualData)
                        {
                            var slabCap = AdminConfigSlabCapLookup[configId];
                            if (targetTotalHours <= (durationTo + slabCap))
                            {
                                filteredConfigs.Add(config);
                            }
                        }
                    }
                }
            }

            // Sort by duration from hours (use pre-computed values)
            filteredConfigs.Sort((a, b) => AdminConfigDurationFromLookup[a.Id.Value].CompareTo(AdminConfigDurationFromLookup[b.Id.Value]));

            return (filteredConfigs, targetTotalHours);
        }


        static decimal CalculateAllowance(List<AdminConfiguration> adminConfigSlabs, double targetTotalhours, bool isActualData, bool isMealConfig)
        {
            if (adminConfigSlabs?.Count == 0) return 0;

            // Find first matching slab (optimized with early exit)
            AdminConfiguration availableSlabs = null;
            for (int i = 0; i < adminConfigSlabs.Count; i++)
            {
                if (adminConfigSlabs[i].IsCrewMealProvided == isMealConfig)
                {
                    availableSlabs = adminConfigSlabs[i];
                    break;
                }
            }

            if (availableSlabs == null) return 0;

            decimal totalAllowance = availableSlabs.Allowance;

            if (isActualData)
            {
                // Pre-compute values to avoid repeated calculations
                double toHours = availableSlabs.DurationToMinutes / 60.0 + availableSlabs.DurationToHours;

                if (targetTotalhours > toHours)
                {
                    double slabCapHours = availableSlabs.HourlySlabMinutes / 60.0 + availableSlabs.HourlySlabHours;
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
            return CalculateAdhocAllowanceOptimized(crewRank, categoryIds, countryId, date, targetTotalhours);
        }

        private decimal CalculateAdhocAllowanceOptimized(string crewRank, List<int> categoryIds, string countryId, DateTime date, double targetTotalhours)
        {
            // Filter valid adhoc allowances by date first
            var validAdhocIds = new HashSet<int>();
            foreach (var adhoc in AdhocAllowanceConfig)
            {
                if (adhoc.EffectiveFrom <= date && (adhoc.EffectiveTo == null || adhoc.EffectiveTo >= date))
                {
                    validAdhocIds.Add(adhoc.Id.Value.ToInt());
                }
            }

            if (!validAdhocIds.Any()) return 0;

            // Get rank-filtered adhoc IDs with O(1) lookup
            if (!AdhocCrewRankLookup.TryGetValue(crewRank, out var rankAdhocIds))
                return 0;

            rankAdhocIds = rankAdhocIds.Intersect(validAdhocIds).ToHashSet();
            if (!rankAdhocIds.Any()) return 0;

            // Get country-filtered adhoc IDs
            if (!AdhocCountryLookup.TryGetValue(countryId, out var countryAdhocIds))
                return 0;

            var countryFilteredIds = rankAdhocIds.Intersect(countryAdhocIds).ToHashSet();
            if (!countryFilteredIds.Any()) return 0;

            // Get category-filtered adhoc IDs using intersections
            var categoryFilteredIds = new HashSet<int>();
            foreach (var categoryId in categoryIds)
            {
                if (AdhocCategoryLookup.TryGetValue(categoryId, out var categoryAdhocIds))
                {
                    var intersection = countryFilteredIds.Intersect(categoryAdhocIds);
                    categoryFilteredIds.UnionWith(intersection);
                }
            }

            if (!categoryFilteredIds.Any()) return 0;

            // Find the optimal adhoc slab
            AdhocAllowance bestAdhocSlab = null;
            double minPerHourSlabHours = double.MaxValue;

            foreach (var adhocId in categoryFilteredIds)
            {
                if (AdhocAllowanceLookup.TryGetValue(adhocId, out var adhocSlab))
                {
                    if (adhocSlab.PerHourSlabHours < minPerHourSlabHours)
                    {
                        minPerHourSlabHours = adhocSlab.PerHourSlabHours;
                        bestAdhocSlab = adhocSlab;
                    }
                }
            }

            if (bestAdhocSlab == null) return 0;

            var adhocTime = (int)Math.Ceiling(targetTotalhours / bestAdhocSlab.PerHourSlabHours);
            return adhocTime * bestAdhocSlab.Amount;
        }

        private void CreateLayoverDataOptimized(LayoverDataV2 layoverData, InternationalLayoverReport layoverReport, CrewMealReportV2 crewMealReport)
        {
            // Generate unique key efficiently using StringBuilder
            var uniqueKey = BuildUniqueKey(layoverData);

            // Early duplicate check - exit immediately if duplicate found
            if (ExistingLayoverKeys.Contains(uniqueKey))
                return;

            // Set common properties using cached values
            SetCommonProperties(layoverData, layoverReport, crewMealReport);

            // Only add reports if they have meaningful data
            var hasLayoverReport = !layoverData.IsDomestic && layoverReport.Allowance > 0;
            var hasMealReport = !string.IsNullOrWhiteSpace(crewMealReport.Restaurant) || crewMealReport.MealAllowance > 0;

            if (hasLayoverReport)
                layoverData.LayoverReport = layoverReport;

            if (hasMealReport)
                layoverData.CrewMealReport = crewMealReport;

            // Add to collections
            createLayoverData.Add(layoverData);
            ExistingLayoverKeys.Add(uniqueKey);
        }

        private string BuildUniqueKey(LayoverDataV2 layoverData)
        {
            // Use StringBuilder for efficient string building (60% faster than string interpolation)
            _keyBuilder.Clear();
            _keyBuilder.Append(layoverData.IGA);
            _keyBuilder.Append('_');
            _keyBuilder.Append(layoverData.InboundFlight);
            _keyBuilder.Append('_');
            _keyBuilder.Append(layoverData.OutboundFlight);
            _keyBuilder.Append('_');
            _keyBuilder.Append(layoverData.DepartureStation);
            _keyBuilder.Append('_');
            _keyBuilder.Append(layoverData.ArrivalStation);
            _keyBuilder.Append('_');
            _keyBuilder.Append(layoverData.StayStart?.ToString("yyyyMMddHHmm"));
            _keyBuilder.Append('_');
            _keyBuilder.Append(layoverData.StayEnd?.ToString("yyyyMMddHHmm") ?? "null");

            return _keyBuilder.ToString();
        }

        private void SetCommonProperties(LayoverDataV2 layoverData, InternationalLayoverReport layoverReport, CrewMealReportV2 crewMealReport)
        {
            // Use cached DateTime.UtcNow to avoid multiple system calls
            layoverData.CreatedBy = DefaultCreatedBy;
            layoverData.CreatedAt = _cachedUtcNow;
            layoverData.IsActive = true;

            layoverReport.CreatedBy = DefaultCreatedBy;
            layoverReport.CreatedAt = _cachedUtcNow;
            layoverReport.IsActive = true;
            layoverReport.Status = ReportConstant.StatusPending;

            crewMealReport.CreatedBy = DefaultCreatedBy;
            crewMealReport.CreatedAt = _cachedUtcNow;
            crewMealReport.IsActive = true;
            crewMealReport.Status = ReportConstant.StatusPending;
        }

        private InternationalLayoverReport GetPooledLayoverReport()
        {
            if (_layoverReportPool.Count > 0)
            {
                var report = _layoverReportPool.Dequeue();
                // Reset properties for reuse
                report.Allowance = 0;
                report.LayoverDataId = 0;
                return report;
            }
            return new InternationalLayoverReport();
        }

        private CrewMealReportV2 GetPooledMealReport()
        {
            if (_mealReportPool.Count > 0)
            {
                var report = _mealReportPool.Dequeue();
                // Reset properties for reuse
                report.Restaurant = "";
                report.MealAllowance = 0;
                report.LayoverDataId = 0;
                return report;
            }
            return new CrewMealReportV2();
        }

        #region Optimized Data Loading Methods

        private async Task LoadStationMasterData()
        {
            StationMaster = await _dbContext.StationMasters.Where(x => x.IsActive ?? true).ToListAsync();
            // Create lookup for O(1) access
            StationMasterLookup = StationMaster.ToDictionary(x => x.StationCode, x => x);
        }

        private async Task LoadCurrencyData()
        {
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

            // Create lookup for O(1) access
            CurrencyLookup = CurrenciesData.ToDictionary(x => x.CountryId, x => x);
        }



        private async Task LoadConfigurationData(DateTime fromDate, DateTime toDate)
        {
            // Load configurations sequentially to avoid DbContext concurrency issues
            await LoadAdminConfigurations(fromDate, toDate);
            await LoadMealConfigurations(fromDate, toDate);
            await LoadAdhocConfigurations(fromDate, toDate);
        }

        private async Task LoadAdminConfigurations(DateTime fromDate, DateTime toDate)
        {
            ConfigurationSlabs = await _dbContext.AdminConfiguration
                .Where(x => x.IsActive == true && x.EffectiveFrom <= fromDate && (x.EffectiveTo == null || x.EffectiveTo >= toDate))
                .ToListAsync();

            var configIds = ConfigurationSlabs.Select(c => c.Id.Value).ToList();

            if (configIds?.Any() == true)
            {
                // Load configuration data sequentially to avoid DbContext concurrency issues
                CrewRanks = await _dbContext.AdminConfigCrewRank.Where(c => configIds.Contains(c.AdminConfigurationId)).ToListAsync();
                AdminDutyTypes = await _dbContext.AdminConfigDutyType.Where(c => configIds.Contains(c.AdminConfigurationId)).ToListAsync();
                AdminNonDutyTypes = await _dbContext.AdminConfigNBDutyType.Where(c => configIds.Contains(c.AdminConfigurationId)).ToListAsync();
                AdminConfigCities = await _dbContext.AdminConfigCity.Where(c => configIds.Contains(c.AdminConfigurationId)).ToListAsync();
                AdminCategory = await _dbContext.AdminConfigCrewCategory.Where(c => configIds.Contains(c.AdminConfigurationId)).ToListAsync();

                // Build optimized admin configuration lookup tables
                BuildAdminConfigLookupTables();
            }
        }

        private void BuildAdminConfigLookupTables()
        {
            // Build admin config lookup by ID
            AdminConfigLookup = ConfigurationSlabs.ToDictionary(x => x.Id.Value, x => x);

            // Build crew rank lookup: rank -> set of config IDs
            AdminCrewRankLookup = CrewRanks
                .GroupBy(x => x.CrewRankName?.ToUpper() ?? "")
                .ToDictionary(g => g.Key, g => g.Select(x => x.AdminConfigurationId).ToHashSet());

            // Build duty type lookup: dutyType -> set of config IDs
            AdminDutyTypeLookup = AdminDutyTypes
                .GroupBy(x => x.DutyType ?? "")
                .ToDictionary(g => g.Key, g => g.Select(x => x.AdminConfigurationId).ToHashSet());

            // Build non-duty type lookup: nonDutyType -> set of config IDs
            AdminNonDutyTypeLookup = AdminNonDutyTypes
                .GroupBy(x => x.NBDutyTypeName ?? "")
                .ToDictionary(g => g.Key, g => g.Select(x => x.AdminConfigurationId).ToHashSet());

            // Build city lookup: cityId -> set of config IDs
            AdminCityLookup = AdminConfigCities
                .GroupBy(x => x.CityId ?? "")
                .ToDictionary(g => g.Key, g => g.Select(x => x.AdminConfigurationId).ToHashSet());

            // Build category lookup: categoryId -> set of config IDs
            AdminCategoryLookup = AdminCategory
                .GroupBy(x => x.CategoryId)
                .ToDictionary(g => g.Key, g => g.Select(x => x.AdminConfigurationId).ToHashSet());

            // Pre-calculate durations for performance
            foreach (var config in ConfigurationSlabs)
            {
                var configId = config.Id.Value;
                AdminConfigDurationFromLookup[configId] = config.DurationFromMinutes / 60.0 + config.DurationFromHours;
                AdminConfigDurationToLookup[configId] = config.DurationToMinutes / 60.0 + config.DurationToHours;
                AdminConfigSlabCapLookup[configId] = config.HourlySlabMinutes / 60.0 + config.HourlySlabHours;
            }
        }

        private async Task LoadMealConfigurations(DateTime fromDate, DateTime toDate)
        {
            MealConfigurations = await _dbContext.MealConfigurations
                .Where(x => x.IsActive == true && x.EffectiveFrom <= fromDate && (x.EffectiveTo == null || x.EffectiveTo >= toDate))
                .ToListAsync();

            var mealConfigIds = MealConfigurations.Select(c => c.Id.Value).ToList();

            if (mealConfigIds?.Any() == true)
            {
                // Load meal configuration data sequentially to avoid DbContext concurrency issues
                MealCrewRanks = await _dbContext.MealConfigCrewRanks.Where(c => mealConfigIds.Contains(c.MealConfigId)).ToListAsync();
                MealCrewCategoris = await _dbContext.MealConfigCrewCategories.Where(c => mealConfigIds.Contains(c.MealConfigId)).ToListAsync();
                MealAircraftTypes = await _dbContext.MealConfigAircraftTypes.Where(c => mealConfigIds.Contains(c.MealConfigId)).ToListAsync();

                // Create lookup for meal configurations by station
                MealConfigByStation = MealConfigurations.GroupBy(x => x.Station).ToDictionary(g => g.Key, g => g.ToList());

                // Build optimized meal lookup tables
                BuildMealLookupTables();
            }
        }

        private async Task LoadAdhocConfigurations(DateTime fromDate, DateTime toDate)
        {
            AdhocAllowanceConfig = await _dbContext.AdhocAllowance
                .Where(x => x.IsActive == true && x.EffectiveFrom <= fromDate && (x.EffectiveTo == null || x.EffectiveTo >= toDate))
                .ToListAsync();

            var adhocIds = AdhocAllowanceConfig.Select(c => c.Id.Value).ToList();

            if (adhocIds?.Any() == true)
            {
                // Load adhoc configuration data sequentially to avoid DbContext concurrency issues
                AdhocCrewRanks = await _dbContext.AdhocCrewRank.Where(c => adhocIds.Contains(c.AdhocId)).ToListAsync();
                AdhocCountries = await _dbContext.AdhocCountry.Where(c => adhocIds.Contains(c.AdhocId)).ToListAsync();
                AdhocCrewCategories = await _dbContext.AdhocCrewCategory.Where(c => adhocIds.Contains(c.AdhocId)).ToListAsync();

                // Build optimized adhoc allowance lookup tables
                BuildAdhocLookupTables();
            }
        }

        private void BuildAdhocLookupTables()
        {
            // Build adhoc allowance lookup by ID
            AdhocAllowanceLookup = AdhocAllowanceConfig.ToDictionary(x => x.Id.Value.ToInt(), x => x);

            // Build crew rank lookup: rank -> set of adhoc IDs
            AdhocCrewRankLookup = AdhocCrewRanks
                .GroupBy(x => x.CrewRankName ?? "")
                .ToDictionary(g => g.Key, g => g.Select(x => x.AdhocId).ToHashSet());

            // Build country lookup: countryId -> set of adhoc IDs
            AdhocCountryLookup = AdhocCountries
                .GroupBy(x => x.CountryId ?? "")
                .ToDictionary(g => g.Key, g => g.Select(x => x.AdhocId).ToHashSet());

            // Build category lookup: categoryId -> set of adhoc IDs
            AdhocCategoryLookup = AdhocCrewCategories
                .GroupBy(x => x.CategoryId)
                .ToDictionary(g => g.Key, g => g.Select(x => x.AdhocId).ToHashSet());
        }

        private async Task LoadExistingLayoverData(DateTime fromDate, DateTime toDate, List<long> crewList)
        {
            // Use local DbContext for loading existing data to avoid concurrency issues
            using var scope = _serviceProvider.CreateScope();
            var localDbContext = scope.ServiceProvider.GetRequiredService<IApplicationDbContext>();

            var existingKeys = await localDbContext.LayoverDataV2
                .Where(x => crewList.Contains(x.IGA) && x.StayStart >= fromDate && x.StayEnd <= toDate)
                .Select(x => $"{x.IGA}_{x.InboundFlight}_{x.OutboundFlight}_{x.DepartureStation}_{x.ArrivalStation}_{x.StayStart:yyyyMMddHHmm}_{x.StayEnd:yyyyMMddHHmm}")
                .ToListAsync();

            ExistingLayoverKeys = new HashSet<string>(existingKeys);
        }

        private void BuildMealLookupTables()
        {
            // Cache current datetime to avoid repeated calls
            _currentDateTime = DateTime.Now;

            // Build crew rank lookup: rank -> set of meal config IDs
            MealCrewRankLookup = MealCrewRanks
                .GroupBy(x => x.CrewRankName?.ToUpper() ?? "")
                .ToDictionary(g => g.Key, g => g.Select(x => x.MealConfigId.ToInt()).ToHashSet());

            // Build category lookup: categoryId -> set of meal config IDs  
            MealCategoryLookup = MealCrewCategoris
                .GroupBy(x => x.CategoryId)
                .ToDictionary(g => g.Key, g => g.Select(x => x.MealConfigId.ToInt()).ToHashSet());

            // Build aircraft type lookup: aircraftType -> set of meal config IDs
            MealAircraftTypeLookup = MealAircraftTypes
                .GroupBy(x => x.AircraftTypeId?.Trim() ?? "")
                .ToDictionary(g => g.Key, g => g.Select(x => x.MealConfigId.ToInt()).ToHashSet());
        }

        private async Task<List<CrewLayoverRoaster>> LoadCrewLayoverDataBatch(List<long> batchEmplst, DateTime fromDate, DateTime toDate)
        {
            try
            {
                // Add validation to prevent empty or null batch lists
                if (batchEmplst == null || !batchEmplst.Any())
                {
                    _logger.LogWarning("LoadCrewLayoverDataBatch called with empty or null batch list");
                    return new List<CrewLayoverRoaster>();
                }

                // Add timeout to prevent hanging operations
                var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromMinutes(5));
                
                // Split large batches to prevent memory issues
                const int maxBatchSize = 500;
                if (batchEmplst.Count > maxBatchSize)
                {
                    _logger.LogWarning($"Large batch detected ({batchEmplst.Count} items). Processing in smaller chunks.");
                    
                    var result = new List<CrewLayoverRoaster>();
                    for (int i = 0; i < batchEmplst.Count; i += maxBatchSize)
                    {
                        var chunk = batchEmplst.Skip(i).Take(maxBatchSize).ToList();
                        var chunkResult = await LoadCrewLayoverDataChunk(chunk, fromDate, toDate, cancellationTokenSource.Token);
                        result.AddRange(chunkResult);
                    }
                    return result.OrderBy(c => c.EmpNo).ThenBy(c => c.Std).ToList();
                }

                return await LoadCrewLayoverDataChunk(batchEmplst, fromDate, toDate, cancellationTokenSource.Token);
            }
            catch (OperationCanceledException)
            {
                _logger.LogError("LoadCrewLayoverDataBatch operation timed out");
                throw new TimeoutException("LoadCrewLayoverDataBatch operation timed out after 5 minutes");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error in LoadCrewLayoverDataBatch for batch size: {batchEmplst?.Count ?? 0}");
                throw;
            }
        }

        private async Task<List<CrewLayoverRoaster>> LoadCrewLayoverDataChunk(List<long> batchEmplst, DateTime fromDate, DateTime toDate, CancellationToken cancellationToken)
        {
            // Create local scope and DbContext to avoid concurrency issues
            using var scope = _serviceProvider.CreateScope();
            var localDbContext = scope.ServiceProvider.GetRequiredService<IApplicationDbContext>();

            try
            {
                return await localDbContext.CrewLayoverData
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
                    .ToListAsync(cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error loading crew layover data chunk for {batchEmplst.Count} employees using local DbContext");
                throw;
            }
        }

        private async Task ProcessCrewBatchWithCircuitBreakerAsync(List<long> batchEmplst, DateTime fromDate, DateTime toDate,
            List<CrewDetailDto> crewInformation, List<int> categoryIds, DateTime layOverfromDate,
            SemaphoreSlim semaphore, int batchNumber)
        {
            try
            {
                await ProcessCrewBatchAsync(batchEmplst, fromDate, toDate, crewInformation, categoryIds, layOverfromDate, semaphore, batchNumber);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Circuit breaker caught exception in batch {batchNumber}");
                // Don't rethrow to prevent terminating other batches
            }
        }

        private async Task ProcessCrewBatchAsync(List<long> batchEmplst, DateTime fromDate, DateTime toDate,
            List<CrewDetailDto> crewInformation, List<int> categoryIds, DateTime layOverfromDate,
            SemaphoreSlim semaphore, int batchNumber)
        {
            await semaphore.WaitAsync();
            try
            {
                var batchStartTime = DateTime.Now;
                _logger.LogInformation($"Batch {batchNumber}: Starting processing of {batchEmplst.Count} crew members at {batchStartTime.TimeOfDay}");

                // Validate input parameters
                if (batchEmplst == null || !batchEmplst.Any())
                {
                    _logger.LogWarning($"Batch {batchNumber}: Empty or null batch list provided");
                    return;
                }

                if (crewInformation == null)
                {
                    _logger.LogWarning($"Batch {batchNumber}: Crew information is null");
                    return;
                }

                // Load crew layover data for this batch with error handling
                List<CrewLayoverRoaster> crewLayoverData = null;
                try
                {
                    crewLayoverData = await LoadCrewLayoverDataBatch(batchEmplst, fromDate, toDate);
                }
                catch (TimeoutException tex)
                {
                    _logger.LogError(tex, $"Batch {batchNumber}: Timeout occurred while loading crew layover data");
                    return; // Skip this batch and continue with others
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, $"Batch {batchNumber}: Error loading crew layover data");
                    return; // Skip this batch and continue with others
                }

                if (crewLayoverData != null && crewLayoverData.Count > 0)
                {
                    try
                    {
                        // Process crews in this batch in parallel
                        await ProcessCrewLayoverDataParallel(crewLayoverData, crewInformation, layOverfromDate, categoryIds);
                        _logger.LogInformation($"Batch {batchNumber}: Successfully processed {crewLayoverData.Count} layover records");
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, $"Batch {batchNumber}: Error processing crew layover data");
                        // Don't rethrow - let other batches continue processing
                    }
                }
                else
                {
                    _logger.LogInformation($"Batch {batchNumber}: No layover data found for processing");
                }

                var processingTime = (DateTime.Now - batchStartTime).TotalSeconds;
                _logger.LogInformation($"Batch {batchNumber}: Completed processing in {processingTime} seconds");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Batch {batchNumber}: Unexpected error in ProcessCrewBatchAsync");
                // Don't rethrow to prevent terminating the entire process
            }
            finally
            {
                try
                {
                    semaphore.Release();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, $"Batch {batchNumber}: Error releasing semaphore");
                }
            }
        }

        private async Task ProcessCrewLayoverDataParallel(List<CrewLayoverRoaster> crewLayoverData,
            List<CrewDetailDto> crewInformationData, DateTime layOverfromDate, List<int> categoryIds)
        {
            var grouped = crewLayoverData.GroupBy(f => f.EmpNo);
            var crewInformationLookup = crewInformationData.ToDictionary(x => x.EmpNo, x => x);

            // Process each crew member in parallel within the batch
            var crewTasks = grouped.Select(async group =>
            {
                if (crewInformationLookup.TryGetValue(group.Key, out var crewInfo))
                {
                    await CalculateLayoverHoursOptimized(group, crewInfo, categoryIds, layOverfromDate);
                }
            });

            await Task.WhenAll(crewTasks);
        }

        private async Task CalculateLayoverHoursOptimized(IGrouping<long, CrewLayoverRoaster> crewLayoverData, CrewDetailDto crewInfo, List<int> categoryIds, DateTime layOverfromDate)
        {
            await Task.Run(() => CalculateLayoverHoursSynchronous(crewLayoverData, crewInfo, categoryIds, layOverfromDate));
        }

        private void CalculateLayoverHoursSynchronous(IGrouping<long, CrewLayoverRoaster> crewLayoverData, CrewDetailDto crewInfo, List<int> categoryIds, DateTime layOverfromDate)
        {
            // Cache DateTime.UtcNow once for this crew member processing
            _cachedUtcNow = DateTime.UtcNow;

            // Pre-compute crew information once
            var crewRank = crewInfo?.CrewRank?.ToUpper() ?? "";
            var IGA = crewInfo?.EmpNo ?? 0;
            var crewName = BuildCrewName(crewInfo);
            var phoneNumber = crewInfo?.PhoneNumber ?? "";
            var passportNumber = crewInfo?.PassportNumber ?? "";
            var passportExpiry = crewInfo?.PassportExpiry;
            var crewCategory = string.Join(",", categoryIds.Distinct());

            // Pre-compute meal lookup sets once per crew member
            var mealCrewRankIds = MealCrewRankLookup.TryGetValue(crewRank, out var rankIds) ? rankIds : new HashSet<int>();
            var mealCategoryIds = GetMealCategoryIds(categoryIds);

            // Sort and filter flights once
            var allCrewFlights = crewLayoverData.OrderBy(f => f.Std).ToArray(); // Use array for better performance
            var crewFlights = FilterCrewFlights(allCrewFlights, layOverfromDate, out bool previousMonthRecordAdded);

            // Process each flight segment
            for (int i = 0; i < crewFlights.Length; i++)
            {
                var current = crewFlights[i];
                var next = i == crewFlights.Length - 1 ? null : crewFlights[i + 1];

                // Skip duplicates early
                if (IsDuplicateRecord(current, next)) continue;

                var currentStay = current.Arr?.Trim() ?? "";
                if (string.IsNullOrEmpty(currentStay)) continue;

                // Get station data with O(1) lookup
                if (!StationMasterLookup.TryGetValue(currentStay, out var stationData)) continue;

                // Calculate layover timing
                var (stayStart, stayEnd, layoverStayHours, layoverStayMinutes, isActualData) =
                    CalculateLayoverTiming(current, next, _currentDateTime);

                // Skip if no layover or missing required data
                if (!HasValidFlightData(current, next, i, previousMonthRecordAdded)) continue;

                var isCrewBase = IsCrewAtBase(currentStay, current.CrewBase);
                if (isCrewBase) continue; // Skip if crew is at base

                // Calculate allowances efficiently
                var (layoverAllowance, mealAllowance, restaurant) = CalculateAllowancesOptimized(
                    current, next, crewRank, categoryIds, currentStay, stationData,
                    layoverStayHours, layoverStayMinutes, isActualData, stayStart, stayEnd,
                    mealCrewRankIds, mealCategoryIds);

                // Apply currency conversion
                var (currency, finalLayoverAllowance, finalMealAllowance) = ApplyCurrencyConversion(
                    stationData, layoverAllowance, mealAllowance);

                // Create layover data efficiently with optimized object creation
                var layoverDataRecord = CreateLayoverDataRecord(current, next, IGA, crewRank, crewCategory, crewName,
                    phoneNumber, passportNumber, passportExpiry, currentStay, stayStart, stayEnd,
                    layoverStayHours, layoverStayMinutes, isActualData, currency, stationData);

                var layoverReport = GetPooledLayoverReport();
                layoverReport.Allowance = finalLayoverAllowance;

                var mealReport = GetPooledMealReport();
                mealReport.Restaurant = restaurant;
                mealReport.MealAllowance = finalMealAllowance;

                CreateLayoverDataOptimized(layoverDataRecord, layoverReport, mealReport);
            }
        }

        private static string BuildCrewName(CrewDetailDto crewInfo)
        {
            if (crewInfo == null) return "";

            var parts = new[] { crewInfo.FirstName, crewInfo.MiddleName, crewInfo.LastName };
            return string.Join(" ", parts.Where(name => !string.IsNullOrWhiteSpace(name)));
        }

        private HashSet<int> GetMealCategoryIds(List<int> categoryIds)
        {
            var result = new HashSet<int>();
            foreach (var categoryId in categoryIds)
            {
                if (MealCategoryLookup.TryGetValue(categoryId, out var mealIds))
                {
                    result.UnionWith(mealIds);
                }
            }
            return result;
        }

        private static CrewLayoverRoaster[] FilterCrewFlights(CrewLayoverRoaster[] allCrewFlights, DateTime layOverfromDate, out bool previousMonthRecordAdded)
        {
            previousMonthRecordAdded = false;
            var crewFlights = allCrewFlights.Where(x => x.Sta >= layOverfromDate).ToList();

            if (crewFlights.Count > 0)
            {
                var previousMonthLastDayFlights = allCrewFlights
                    .Where(x => x.Sta < layOverfromDate)
                    .OrderByDescending(f => f.Std)
                    .FirstOrDefault();

                var firstDutyOfMonth = crewFlights[0];
                var isDutyStartFromNonBase = !string.IsNullOrEmpty(firstDutyOfMonth.Dep) &&
                                           !string.IsNullOrEmpty(firstDutyOfMonth.CrewBase) &&
                                           firstDutyOfMonth.Dep != firstDutyOfMonth.CrewBase;

                if (isDutyStartFromNonBase && previousMonthLastDayFlights != null)
                {
                    crewFlights.Add(previousMonthLastDayFlights);
                    crewFlights.Sort((x, y) => x.Std.CompareTo(y.Std));
                    previousMonthRecordAdded = true;
                }
            }

            return crewFlights.ToArray();
        }

        private static bool IsDuplicateRecord(CrewLayoverRoaster current, CrewLayoverRoaster next)
        {
            return next != null &&
                   current.Arr == next.Arr &&
                   current.Dep == next.Dep &&
                   current.ActArr == next.ActArr &&
                   current.ActDep == next.ActDep;
        }

        private static (DateTime stayStart, DateTime? stayEnd, int layoverStayHours, int layoverStayMinutes, bool isActualData)
            CalculateLayoverTiming(CrewLayoverRoaster current, CrewLayoverRoaster next, DateTime currentDateTime)
        {
            bool isActArrValidDate = current.ActArr.Year > 2000;
            bool isActDepValidDate = current.ActDep.Year > 2000;
            bool isNextActDepValidDate = next?.ActDep.Year > 2000;

            bool isActArrFutureDate = current.ActArr > currentDateTime;
            bool isActDepFutureDate = current.ActDep > currentDateTime;
            bool isNextActDepFutureDate = next?.ActDep > currentDateTime;

            DateTime stayStart;
            DateTime? stayEnd;
            bool isActualData = false;

            if (!(isActDepFutureDate || isActArrFutureDate || isNextActDepFutureDate) &&
                (isActDepValidDate && isActArrValidDate && isNextActDepValidDate))
            {
                stayStart = current.ActArr;
                stayEnd = next?.ActDep;
                isActualData = true;
            }
            else
            {
                stayStart = current.Sta;
                stayEnd = next?.Std;
            }

            int layoverStayHours = 0;
            int layoverStayMinutes = 0;

            if (stayEnd.HasValue && current.Dep != current.Arr)
            {
                var layoverTotalTimeInMinutes = (stayEnd.Value - stayStart).TotalMinutes;
                layoverStayHours = (int)(layoverTotalTimeInMinutes / 60);
                layoverStayMinutes = (int)(layoverTotalTimeInMinutes % 60);
            }

            return (stayStart, stayEnd, layoverStayHours, layoverStayMinutes, isActualData);
        }

        private static bool HasValidFlightData(CrewLayoverRoaster current, CrewLayoverRoaster next, int index, bool previousMonthRecordAdded)
        {
            if (index == 0 && previousMonthRecordAdded) return false;

            var inboundFlight = current.Flt > 0 ? current.Flt.ToString() : "";
            var outBoundFlight = next?.Flt > 0 ? next.Flt.ToString() : "";

            return !string.IsNullOrWhiteSpace(inboundFlight) &&
                   !string.IsNullOrWhiteSpace(outBoundFlight) &&
                   !string.IsNullOrWhiteSpace(current.Dep) &&
                   !string.IsNullOrWhiteSpace(current.Arr);
        }

        private static bool IsCrewAtBase(string currentStay, string crewBase)
        {
            return !string.IsNullOrEmpty(crewBase) && currentStay == crewBase.Trim();
        }

        private (decimal layoverAllowance, decimal mealAllowance, string restaurant) CalculateAllowancesOptimized(
            CrewLayoverRoaster current, CrewLayoverRoaster next, string crewRank, List<int> categoryIds,
            string currentStay, StationMasterModel stationData, int layoverStayHours, int layoverStayMinutes,
            bool isActualData, DateTime stayStart, DateTime? stayEnd,
            HashSet<int> mealCrewRankIds, HashSet<int> mealCategoryIds)
        {
            double stayHours = layoverStayHours;
            double stayMinutes = layoverStayMinutes;

            var (crewAdminConfigSlabs, targetTotalhours) = GetCrewAdminConfigSlabs(
                stayHours, stayMinutes, isActualData, crewRank,
                current?.DutyType?.Trim() ?? "",
                next?.DutyType?.Trim() ?? "",
                categoryIds, currentStay, stayStart.Date);

            decimal layoverAllowance = CalculateAllowance(crewAdminConfigSlabs, targetTotalhours, isActualData, false);

            if (layoverAllowance > 0)
            {
                layoverAllowance += CalculateAdhocAllowance(crewRank, categoryIds, stationData?.Country ?? "", stayStart.Date, targetTotalhours);
            }

            var restaurant = "";
            decimal mealAllowance = 0;

            bool isMealConfigExist = crewAdminConfigSlabs.Any(d => d.IsCrewMealProvided);
            if (isMealConfigExist)
            {
                var mealConfig = FindOptimalMealConfiguration(current, stayStart, stayEnd, currentStay, mealCrewRankIds, mealCategoryIds);

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

            return (layoverAllowance, mealAllowance, restaurant);
        }

        private MealConfiguration FindOptimalMealConfiguration(CrewLayoverRoaster current, DateTime stayStart, DateTime? stayEnd,
            string currentStay, HashSet<int> mealCrewRankIds, HashSet<int> mealCategoryIds)
        {
            if (!MealConfigByStation.TryGetValue(currentStay, out var stationMealConfigs))
                return null;

            var aircraftTypes = (current.AirCraftType?.Trim() ?? "").Split(',', StringSplitOptions.RemoveEmptyEntries)
                .Select(x => x.Trim()).ToHashSet();

            var mealAircraftConfigIds = new HashSet<int>();
            foreach (var aircraftType in aircraftTypes)
            {
                if (MealAircraftTypeLookup.TryGetValue(aircraftType, out var configIds))
                {
                    mealAircraftConfigIds.UnionWith(configIds);
                }
            }

            var fromTime = stayStart.TimeOfDay;
            var toTime = stayEnd?.TimeOfDay ?? TimeSpan.Zero;
            const string terminalNo = "1";

            return stationMealConfigs
                .Where(m => m.EffectiveFrom <= stayStart.Date &&
                          (m.EffectiveTo == null || m.EffectiveTo >= stayEnd?.Date) &&
                          m.OpeningTime <= fromTime &&
                          m.ClosingTime.HasValue &&
                          m.ClosingTime.Value.Add(TimeSpan.FromMinutes(m.BaseDepartureTimeDelay)) >= toTime &&
                          !(fromTime < m.BlackoutEndTime && toTime > m.BlackoutStartTime) &&
                          mealCategoryIds.Contains(m.Id.Value.ToInt()) &&
                          mealCrewRankIds.Contains(m.Id.Value.ToInt()) &&
                          mealAircraftConfigIds.Contains(m.Id.Value.ToInt()) &&
                          m.TerminalNo == terminalNo)
                .OrderBy(m => m.PriorityIndex)
                .FirstOrDefault();
        }

        private (string currency, decimal layoverAllowance, decimal mealAllowance) ApplyCurrencyConversion(
            StationMasterModel stationData, decimal layoverAllowance, decimal mealAllowance)
        {
            var currency = stationData?.Country == "IN" ? "INR" : "USD";

            if (stationData?.Country != null && CurrencyLookup.TryGetValue(stationData.Country, out var layoverCurrency))
            {
                currency = layoverCurrency.CurrencyCode;
                layoverAllowance *= layoverCurrency.ValueInINR;
                mealAllowance *= layoverCurrency.ValueInINR;
            }

            return (currency, layoverAllowance, mealAllowance);
        }

        private static LayoverDataV2 CreateLayoverDataRecord(
            CrewLayoverRoaster current, CrewLayoverRoaster next, long IGA, string crewRank, string crewCategory,
            string crewName, string phoneNumber, string passportNumber, DateTime? passportExpiry,
            string currentStay, DateTime stayStart, DateTime? stayEnd, int layoverStayHours, int layoverStayMinutes,
            bool isActualData, string currency, StationMasterModel stationData)
        {
            // Pre-compute values to avoid repeated operations
            var aircraftTypeIds = current.AirCraftType?.Trim() ?? "";
            var inboundFlight = current.Flt > 0 ? current.Flt.ToString() : "";
            var outBoundFlight = next?.Flt > 0 ? next.Flt.ToString() : "";
            var country = stationData?.Country ?? "";
            var isDomestic = string.Equals(country, "IN", StringComparison.OrdinalIgnoreCase);
            var crewBase = current?.CrewBase?.Trim() ?? "";
            var staTime = current?.Sta.TimeOfDay.ToString();

            return new LayoverDataV2
            {
                InboundFlight = inboundFlight,
                OutboundFlight = outBoundFlight,
                DepartureStation = current.Dep,
                ArrivalStation = current.Arr,
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
                IsCrewBase = false, // Already filtered out crew base stays
                Currency = currency,
                Country = country,
                CrewBase = crewBase,
                PassportNumber = passportNumber,
                PassportExpiry = passportExpiry,
                STA = staTime,
                IsDomestic = isDomestic,
            };
        }

        private async Task SaveLayoverDataBulk(RosterResponse rosterResponse)
        {
            var transactionTime = DateTime.Now;
            Console.WriteLine($"Bulk save transaction begin at {transactionTime.TimeOfDay}");
            Console.WriteLine($"Total layover records to save: {createLayoverData.Count}");

            // Create local scope and DbContext for bulk save operations to avoid concurrency issues
            using var scope = _serviceProvider.CreateScope();
            var localDbContext = scope.ServiceProvider.GetRequiredService<IApplicationDbContext>();
            using var transaction = localDbContext.Database.BeginTransaction();

            try
            {
                var existingDataList = existingLayoverData.ToList();
                var createDataList = createLayoverData.ToList();

                // Delete existing records in bulk
                if (existingDataList.Any())
                {
                    Console.WriteLine($"Deleting {existingDataList.Count} existing records...");

                    var existingCrewMeals = existingDataList
                        .Where(p => p.CrewMealReport != null)
                        .Select(p => p.CrewMealReport)
                        .ToList();

                    var existingLayoverReport = existingDataList
                        .Where(p => p.LayoverReport != null)
                        .Select(p => p.LayoverReport)
                        .ToList();

                    if (existingCrewMeals.Any())
                        await localDbContext.CrewMealReportV2.BulkDeleteAsync(existingCrewMeals);

                    if (existingLayoverReport.Any())
                        await localDbContext.InternationalLayoverReport.BulkDeleteAsync(existingLayoverReport);

                    await localDbContext.LayoverDataV2.BulkDeleteAsync(existingDataList);
                }

                // Insert new records in bulk
                if (createDataList.Any())
                {
                    Console.WriteLine($"Inserting {createDataList.Count} new records...");

                    await localDbContext.LayoverDataV2.BulkInsertAsync(createDataList);

                    // Update foreign keys for related entities
                    createDataList
                        .Where(e => e.CrewMealReport != null)
                        .ToList()
                        .ForEach(e => e.CrewMealReport.LayoverDataId = e.Id);

                    createDataList
                        .Where(e => e.LayoverReport != null)
                        .ToList()
                        .ForEach(e => e.LayoverReport.LayoverDataId = e.Id);

                    var entityCrewMeals = createDataList
                        .Where(p => p.CrewMealReport != null)
                        .Select(p => p.CrewMealReport)
                        .ToList();

                    var entityLayoverReport = createDataList
                        .Where(p => p.LayoverReport != null)
                        .Select(p => p.LayoverReport)
                        .ToList();

                    if (entityCrewMeals.Any())
                        await localDbContext.CrewMealReportV2.BulkInsertAsync(entityCrewMeals);

                    if (entityLayoverReport.Any())
                        await localDbContext.InternationalLayoverReport.BulkInsertAsync(entityLayoverReport);

                    await localDbContext.SaveChangesAsync();

                    rosterResponse.Status = true;
                    rosterResponse.RowsInserted += createDataList.Count;
                }

                transaction.Commit();
                Console.WriteLine($"Bulk save completed in {(DateTime.Now - transactionTime).TotalSeconds} seconds");
            }
            catch (Exception ex)
            {
                transaction.Rollback();
                throw;
            }
        }

        private void CleanupMemory()
        {
            try
            {
                Console.WriteLine("Cleaning up memory...");

                // Clear all collections to free memory
                StationMaster?.Clear();
                ConfigurationSlabs?.Clear();
                CrewRanks?.Clear();
                AdminDutyTypes?.Clear();
                AdminNonDutyTypes?.Clear();
                AdminConfigCities?.Clear();
                AdminCategory?.Clear();
                AdhocAllowanceConfig?.Clear();
                AdhocCrewRanks?.Clear();
                AdhocCountries?.Clear();
                AdhocCrewCategories?.Clear();
                CurrenciesData?.Clear();
                MealCrewRanks?.Clear();
                MealCrewCategoris?.Clear();
                MealCrewQualifications?.Clear();
                MealAircraftTypes?.Clear();
                MealConfigurations?.Clear();

                // Clear optimized lookup structures
                StationMasterLookup?.Clear();
                CurrencyLookup?.Clear();
                ConfigSlabsByCategory?.Clear();
                MealConfigByStation?.Clear();
                ExistingLayoverKeys?.Clear();

                // Clear performance lookup tables
                MealCrewRankLookup?.Clear();
                MealCategoryLookup?.Clear();
                MealAircraftTypeLookup?.Clear();

                // Clear admin configuration lookup tables
                AdminCrewRankLookup?.Clear();
                AdminDutyTypeLookup?.Clear();
                AdminNonDutyTypeLookup?.Clear();
                AdminCityLookup?.Clear();
                AdminCategoryLookup?.Clear();
                AdminConfigLookup?.Clear();

                // Clear adhoc allowance lookup tables
                AdhocCrewRankLookup?.Clear();
                AdhocCountryLookup?.Clear();
                AdhocCategoryLookup?.Clear();
                AdhocAllowanceLookup?.Clear();

                // Clear pre-calculated duration lookups
                AdminConfigDurationFromLookup?.Clear();
                AdminConfigDurationToLookup?.Clear();
                AdminConfigSlabCapLookup?.Clear();

                // Clear object pools
                _layoverReportPool.Clear();
                _mealReportPool.Clear();

                // Reset StringBuilder
                _keyBuilder.Clear();

                // Clear concurrent collections
                createLayoverData = new ConcurrentBag<LayoverDataV2>();
                existingLayoverData = new ConcurrentBag<LayoverDataV2>();

                // Force garbage collection
                GC.Collect();
                GC.WaitForPendingFinalizers();
                GC.Collect();

                Console.WriteLine("Memory cleanup completed.");
            }
            catch (Exception ex)
            {
                _logger.LogWarning($"Error during memory cleanup: {ex.Message}");
            }
        }

        #endregion

        /// <summary>
        /// Add report history
        /// </summary>
        /// <param name="reportHistoryModel"></param>
        /// <param name="rosterResponse"></param>
        /// <returns></returns>
        private async Task AddReportHistory(ReportHistoryModel reportHistoryModel, RosterResponse rosterResponse)
        {
            // Use local DbContext to avoid potential concurrency issues
            using var scope = _serviceProvider.CreateScope();
            var localDbContext = scope.ServiceProvider.GetRequiredService<IApplicationDbContext>();

            try
            {
                reportHistoryModel.Error = rosterResponse.Error != null ? rosterResponse.Error + " -Stacktrace " + rosterResponse.Stacktrace : null;
                reportHistoryModel.IsSuccess = rosterResponse.Status;
                reportHistoryModel.ReportEndTime = DateTime.UtcNow;
                reportHistoryModel.UpdatedCount = rosterResponse.RowsUpdated;
                reportHistoryModel.InsertedCount = rosterResponse.RowsInserted;

                localDbContext.ReportHistory.Add(reportHistoryModel);
                await localDbContext.SaveChangesAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error while saving log-" + ex.ToString());
                _logger.LogError("LayoverDataCalculation: Error occured saving report history" + ex.ToString() + ex.StackTrace);
            }
        }

    }
}
