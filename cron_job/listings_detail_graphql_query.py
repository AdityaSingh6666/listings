LISTING_DETAIL_QUERY = '''
query getListingDetails($listingId: Int!, $include: [ListingInclusion]) {
  listingDetails(id: $listingId, include: $include) {
    ...LISTING
    ... on ListingResultError {
      errorCode
      message
    }
  }
}

fragment LISTING on ListingData {
  listingId
  administrationFees
  detailedDescription
  metaTitle
  metaDescription
  category
  listingUris {
    detail
  }
  title
  publicationStatus
  counts {
    numBedrooms
    numBathrooms
    numLivingRooms
  }
  viewCount {
    viewCount30day
  }
  ntsInfo {
    title
    value
  }
  derivedEPC {
    efficiencyRating
  }
  ...AGENT_BRANCH
  ...LISTING_ANALYTICS_TAXONOMY
  ...LISTING_ADTARGETING
  ...LISTING_ANALYTICS_ECOMMERCE
  ...PRICING
  ...ENERGY_PERFORMANCE_CERTIFICATE
  ...LISTING_FEATURES
  ...FLOOR_PLANS
  ...FLOOR_AREA
  ...MEDIA
  ...MAP
  ...EMBEDDED_CONTENT
  ...POINTS_OF_INTEREST
  ...PRICE_HISTORY
  ...LISTING_SUMMARY
}

fragment AGENT_BRANCH on ListingData {
  branch {
    ...AGENT_BRANCH_FRAGMENT
  }
}

fragment AGENT_BRANCH_FRAGMENT on AgentBranch {
  branchId
  address
  branchDetailsUri
  branchResultsUri
  logoUrl
  phone
  name
  postcode
  memberType
  attributes {
    embeddedContentIsBlacklisted
    showOverseasListingExactLocation
  }
  profile {
    details
  }
}

fragment LISTING_ANALYTICS_TAXONOMY on ListingData {
  analyticsTaxonomy {
    ...LISTING_ANALYTICS_TAXONOMY_FRAGMENT
  }
}

fragment LISTING_ANALYTICS_TAXONOMY_FRAGMENT on ListingAnalyticsTaxonomy {
  acorn
  acornType
  areaName
  bedsMax
  bedsMin
  branchId
  branchLogoUrl
  branchName
  brandName
  chainFree
  companyId
  countryCode
  countyAreaName
  currencyCode
  displayAddress
  furnishedState
  groupId
  hasEpc
  hasFloorplan
  incode
  isRetirementHome
  isSharedOwnership
  listingCondition
  listingId
  listingsCategory
  listingStatus
  location
  memberType
  numBaths
  numBeds
  numImages
  numRecepts
  outcode
  postalArea
  postTownName
  priceActual
  price
  priceMax
  priceMin
  priceQualifier
  propertyHighlight
  propertyType
  regionName
  section
  sizeSqFeet
  tenure
  uuid
  zindex
}

fragment LISTING_ADTARGETING on ListingData {
  adTargeting {
    ...LISTING_ANALYTICS_TAXONOMY_FRAGMENT
  }
}

fragment LISTING_ANALYTICS_ECOMMERCE on ListingData {
  analyticsEcommerce {
    ...LISTING_ANALYTICS_ECOMMERCE_FRAGMENT
  }
}

fragment LISTING_ANALYTICS_ECOMMERCE_FRAGMENT on ListingAnalyticsEcommerce {
  brand
  category
  id
  name
  price
  quantity
  variant
}

fragment PRICING on ListingData {
  pricing {
    ...PRICING_FRAGMENT
  }
}

fragment PRICING_FRAGMENT on ListingPricing {
  isAuction
  qualifier
  priceQualifierLabel
  internalValue
  rentFrequencyLabel
  valueLabel
  currencyCode
  originalCurrencyPrice {
    internalValue
    rentFrequencyLabel
    unitsLabel
    label
    valueLabel
    currencyCode
  }
  pricePerFloorAreaUnit {
    internalValue
    rentFrequencyLabel
    unitsLabel
    label
    valueLabel
    currencyCode
  }
  alternateRentFrequencyPrice {
    internalValue
    rentFrequencyLabel
    unitsLabel
    label
    valueLabel
    currencyCode
  }
}

fragment ENERGY_PERFORMANCE_CERTIFICATE on ListingData {
  epc {
    image {
      caption
      filename
    }
    pdf {
      caption
      original
    }
  }
}

fragment LISTING_FEATURES on ListingData {
  detailedDescription
  features {
    ...LISTING_FEATURES_FRAGMENT
  }
}

fragment LISTING_FEATURES_FRAGMENT on Features {
  bullets
  flags {
    furnishedState {
      name
      label
    }
    studentFriendly
    tenure {
      name
      label
    }
    availableFromDate
  }
  highlights {
    description
    label
  }
}

fragment FLOOR_PLANS on ListingData {
  floorPlan {
    ...FLOOR_PLANS_FRAGMENT
  }
}

fragment FLOOR_PLANS_FRAGMENT on FloorPlan {
  image {
    filename
    caption
  }
  links {
    url
    label
  }
  pdf {
    original
    caption
  }
}

fragment FLOOR_AREA on ListingData {
  floorArea {
    ...FLOOR_AREA_FRAGMENT
  }
}

fragment FLOOR_AREA_FRAGMENT on FloorArea {
  label
  range {
    maxValue
    maxValueLabel
    minValue
    minValueLabel
  }
  units
  unitsLabel
  value
}

fragment MEDIA on ListingData {
  content {
    virtualTour {
      ...MEDIA_FRAGMENT
    }
    floorPlan {
      ...MEDIA_FRAGMENT
    }
    audioTour {
      ...MEDIA_FRAGMENT
    }
  }
  propertyImage {
    ...MEDIA_FRAGMENT
  }
  additionalLinks {
    ...MEDIA_FRAGMENT
    ... on AdditionalLink {
      href
      label
    }
  }
}

fragment MEDIA_FRAGMENT on Media {
  original
  caption
  url
  filename
  type
}

fragment MAP on ListingData {
  location {
    ...LISTING_LOCATION_FRAGMENT
  }
}

fragment LISTING_LOCATION_FRAGMENT on ListingLocation {
  coordinates {
    isApproximate
    latitude
    longitude
  }
  postalCode
  streetName
  countryCode
  propertyNumberOrName
  townOrCity
}

fragment EMBEDDED_CONTENT on ListingData {
  embeddedContent {
    videos {
      ...MEDIA_FRAGMENT
    }
    tours {
      ...MEDIA_FRAGMENT
    }
    links {
      ...MEDIA_FRAGMENT
    }
  }
}

fragment POINTS_OF_INTEREST on ListingData {
  pointsOfInterest {
    ...POINTS_OF_INTEREST_FRAGMENT
  }
}

fragment POINTS_OF_INTEREST_FRAGMENT on PointOfInterest {
  title
  address
  type
  latitude
  longitude
  distanceMiles
}

fragment PRICE_HISTORY on ListingData {
  priceHistory {
    firstPublished {
      firstPublishedDate
      priceLabel
    }
    lastSale {
      date
      newBuild
      price
      priceLabel
      recentlySold
    }
    priceChanges {
      isMinorChange
      isPriceDrop
      isPriceIncrease
      percentageChangeLabel
      priceChangeDate
      priceChangeLabel
      priceLabel
    }
  }
}

fragment LISTING_SUMMARY on ListingData {
  listingId
  displayAddress
  category
  location {
    postalCode
    streetName
    uprn
  }
  section
  branch {
    logoUrl
    name
  }
  counts {
    numBedrooms
    numBathrooms
    numLivingRooms
  }
  featurePreview {
    iconId
    content
  }
  floorArea {
    label
    range {
      maxValue
      maxValueLabel
      minValue
      minValueLabel
    }
    units
    unitsLabel
    value
  }
  imagePreview {
    caption
    src
  }
  propertyImage {
    caption
    original
  }
  listingUris {
    contact
    detail
  }
  pricing {
    label
    internalValue
  }
  tags {
    label
  }
  title
  transports {
    distanceInMiles
    poiType
    title
  }
  publicationStatus
  publishedOn
  numberOfImages
  statusSummary {
    label
  }
  ...LISTING_ANALYTICS_TAXONOMY
}
'''