/**
 * WildcardMatcher
 * Utility for matching text against wildcard patterns
 */

#ifndef SRCLIENT_SERDES_WILDCARD_MATCHER_H_
#define SRCLIENT_SERDES_WILDCARD_MATCHER_H_

#include <string>
#include <regex>
#include <utility>

namespace srclient::serdes {

/**
 * Match text against a wildcard pattern
 * 
 * Supports:
 * - '*' matches any sequence of characters except separator
 * - '**' matches any sequence of characters including separator
 * - '?' matches any single character except separator
 * 
 * @param text The text to match against
 * @param matcher The wildcard pattern to match
 * @return true if the text matches the pattern exactly
 */
bool wildcardMatch(const std::string& text, const std::string& matcher);

/**
 * Convert a wildcard pattern to a regular expression
 * 
 * @param pattern The wildcard pattern
 * @param separator The separator character (default ".")
 * @return The regular expression string
 */
std::string wildcardToRegexp(const std::string& pattern, const std::string& separator = ".");

} // namespace srclient::serdes

#endif // SRCLIENT_SERDES_WILDCARD_MATCHER_H_ 