# Overview

This is an aircraft tracking and visualization system focused on helicopter monitoring using ADS-B (Automatic Dependent Surveillance-Broadcast) data. The application consists of two main components: a Node.js backend server that collects and processes real-time aircraft position data, and a web-based frontend for displaying aircraft information, flight events, and interactive visualizations. The system includes features for tracking takeoffs/landings, managing aircraft profiles, sending notifications, and providing both live tracking and historical data analysis through charts and maps.

# User Preferences

Preferred communication style: Simple, everyday language.

# System Architecture

## Backend Architecture
- **Node.js HTTP Server**: Custom HTTP server handling API endpoints, static file serving, and Server-Sent Events (SSE) for real-time data streaming
- **Data Collection**: Uses Puppeteer for web scraping to gather ADS-B data from external sources
- **File-based Storage**: JSON files store aircraft logs, events, configuration, and places data with automatic file management and rotation
- **Event Processing**: Real-time detection of takeoff/landing events based on altitude and speed thresholds with configurable parameters
- **Place Matching**: Geographic location matching using OpenStreetMap integration for event location identification

## Frontend Architecture
- **Progressive Web App (PWA)**: Modern web application with offline capabilities, service worker caching, and mobile app-like experience
- **Real-time Updates**: Server-Sent Events for live data streaming without constant polling
- **Interactive Visualizations**: Leaflet.js for mapping, Chart.js for flight data visualization, custom CSS animations for aircraft displays
- **Responsive Design**: Tailwind CSS framework with mobile-first approach and adaptive layouts

## Data Storage Solutions
- **Log Management**: Individual JSON files per aircraft with automatic log rotation and configurable retention limits
- **Event Storage**: Centralized events.json with takeoff/landing records and associated location data
- **Configuration Management**: Modular JSON configuration files for thresholds, notifications, and application settings
- **History Archives**: Separate directory structure for long-term data storage with organized file naming conventions

## Authentication and Authorization
- **Basic Authentication**: Simple header-based authentication for historical data access
- **No User Management**: Single-user system focused on simplicity over complex user management

## External Dependencies
- **Puppeteer**: Headless Chrome automation for web scraping ADS-B data sources
- **OpenStreetMap API**: Reverse geocoding for location name resolution
- **Chart.js**: Client-side charting library for flight data visualization
- **Leaflet.js**: Interactive mapping library for geographic displays
- **Tailwind CSS**: Utility-first CSS framework for responsive design

## Additional Components
- **A320 PFD Simulator**: Embedded React-based Primary Flight Display simulation with realistic aircraft instrumentation, supporting multiple input methods (keyboard, gamepad, gyroscope) for flight simulation experiences