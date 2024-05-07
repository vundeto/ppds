/*
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package org.ppds.core;

public interface ONCIterator {
	/**
	 * Initializes the iterator, and allocates necessary resources.
	 */
	public void open();

	/**
	 * Returns the next qualifying record or null to indicate end-of-file (EOF).
	 * The returned records can be internally reused on any subsequent next call.
	 * 
	 * @return next record, null for EOF
	 */
	public Record next();
	
	/**
	 * Closes the iterator, and frees any resources allocated during
	 * invocations of open or next.
	 */
	public void close();
}
